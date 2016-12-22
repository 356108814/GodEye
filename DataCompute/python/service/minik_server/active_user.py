# encoding: utf-8
"""
minik活跃用户统计
@author Yuriseus
@create 16-11-03 下午4:23
"""
import json

from common.db_source import DBSource
from common.hbase_client import HbaseClient
from config.enums import WEEKDAY, Product
from util.date import DateUtil

hbase_client = HbaseClient()
db_minik = DBSource.minik()


def get_table_au_metrics():
    return hbase_client.table('active_user_metrics')


class ActiveUserService(object):
    def __init__(self):
        pass

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            d['data'] = json.loads(tmp[3])    # 字典
        except Exception:
            d['data'] = None
        d['exec_version'] = tmp[4]    # 程序版本
        d['ui_version'] = tmp[5]    # UI版本
        return d

    def process_data(self, d):
        """
        处理data数据，分解data字典，去除空数据
        :param d:
        :return:
        """
        data = d['data']
        del d['data']
        for k, v in data.items():
            if v == '' or (isinstance(v, list) and not v):
                continue
            d[k] = v
        # 增加登陆日期
        d['date'] = data['player']['current_login_time']
        d['timestamp'] = DateUtil.date2timestamp(d['date'], '%Y-%m-%d %H:%M:%S')
        d['day'] = d['date'][0:10]    # 用于统计
        # 增加星期
        weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        uid = data['player']['uid']    # str
        d['uid'] = uid
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'onWebxinLogin' and data:
            return True
        return False

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        self.count_day_metrics(rdd, sql_context)
        hbase_client.close()

    def save_to_active_user(self, rdd):
        """
        保存到hbase活跃库
        :param rdd:
        :return:
        """
        datas = rdd.collect()
        table_au = hbase_client.table('active_user')
        for d in datas:
            data = {}
            uid = d['uid'].zfill(8)
            timestamp = DateUtil.date2timestamp(d['date'], '%Y-%m-%d %H:%M:%S')
            row_key = '%(product)s-%(timestamp)s-%(uid)s' % {'product': Product.MINIK, 'timestamp': timestamp,
                                                             'uid': uid}
            for k, v in d.items():
                if k in ['date', 'weekday', 'uid', 'mid']:
                    column = '%s:%s' % ('info', k)
                    data[column] = v
            table_au.put(row_key, data)

    def count_day_metrics(self, rdd, sql_context):
        """
        统计天活跃指标
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        df.printSchema()
        df.registerTempTable("login")

        metrics_dict = {}
        # 统计登录次数
        sql = "SELECT day, weekday, COUNT(uid) AS count FROM login GROUP BY day, weekday ORDER BY day"
        dur_df = sql_context.sql(sql)
        for row in dur_df.collect():
            row_dict = row.asDict()
            # 构造需要保存的指标字典
            date = row_dict['day']
            row_key = '%(product)s-%(type)s-%(date)s' % {'product': Product.MINIK, 'type': 'day',
                                                         'date': date.replace('-', '')}
            if row_key not in metrics_dict:
                metrics_dict[row_key] = {
                    'login_count': 0,
                    'user_count': 0
                }
            md = metrics_dict[row_key]
            md['date'] = date
            md['weekday'] = row_dict['weekday']
            login_count = row_dict['count']
            md['login_count'] += login_count

        # 统计登录人次
        sql = "SELECT day, weekday, COUNT(DISTINCT uid) AS count FROM login GROUP BY day, weekday ORDER BY day"
        dur_df = sql_context.sql(sql)
        rows = dur_df.collect()
        for row in rows:
            row_dict = row.asDict()
            # 构造需要保存的指标字典
            date = row_dict['day']
            row_key = '%(product)s-%(type)s-%(date)s' % {'product': Product.MINIK, 'type': 'day',
                                                         'date': date.replace('-', '')}
            md = metrics_dict[row_key]
            md['date'] = date
            md['weekday'] = row_dict['weekday']
            user_count = row_dict['count']
            md['user_count'] += user_count

            # 增加新增、流失、累计用户数、活跃率
            add_user, lost_user, ac_user = self.get_a_l_ac(row_key, date)
            md['add_user'] = add_user
            md['lost_user'] = lost_user
            md['total_user'] = ac_user
            md['rate'] = '%.6f' % (user_count/float(ac_user))
        self.update_day_metrics(metrics_dict)
        db_minik.close()

    def get_a_l_ac(self, row_key, date):
        """
        获取新增、流失、累计用户数
        :param row_key:
        :param date:
        :return:
        """
        try:
            table_au_metrics = get_table_au_metrics()
            row = table_au_metrics.row(row_key, ['info:add_user', 'info:lost_user', 'info:total_user'])
        except Exception as e:
            row = None
        if row and int(row['info:add_user']) > 0:
            add_user = int(row['info:add_user'])
            lost_user = int(row['info:lost_user'])
            ac_user = int(row['info:total_user'])
        else:
            add_user = self.get_subscribe_count(date)
            lost_user = self.get_unsubscribe_count(date)
            ac_user = self.get_accumulative_count(date)
        return add_user, lost_user, ac_user

    def get_subscribe_count(self, date):
        """
        获取新订阅用户数
        :param date:
        :return:
        """
        start_timestamp = DateUtil.date2timestamp('%s 00:00:00' % date, '%Y-%m-%d %H:%M:%S')
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT count(*) AS count FROM t_baseinfo_user WHERE subscribeTime >= %s AND subscribeTime <= %s" % \
              (start_timestamp, end_timestamp)
        count = int(db_minik.query(sql)[0]['count'])
        return count

    def get_unsubscribe_count(self, date):
        """
        获取取消订阅用户数
        :param date:
        :return:
        """
        start_timestamp = DateUtil.date2timestamp('%s 00:00:00' % date, '%Y-%m-%d %H:%M:%S')
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT count(*) AS count FROM t_baseinfo_user WHERE unsubscribeTime >= %s AND unsubscribeTime <= %s" % \
              (start_timestamp, end_timestamp)
        count = int(db_minik.query(sql)[0]['count'])
        return count

    def get_accumulative_count(self, date):
        """
        获取累计用户数
        :param date:
        :return:
        """
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT count(*) AS count FROM t_baseinfo_user WHERE subscribeTime <= %s" % end_timestamp
        count = int(db_minik.query(sql)[0]['count'])
        return count

    def update_day_metrics(self, metrics_dict):
        """
        更新DAU指标表
        :param metrics_dict: dict字典
        :return:
        """
        table_au_metrics = get_table_au_metrics()
        for row_key, metrics in metrics_dict.items():
            cf = 'info'
            data = {}
            for k, v in metrics.items():
                column = '%s:%s' % (cf, k)
                if k in ['login_count', 'user_count']:
                    old = 0
                    try:
                        column_dict = table_au_metrics.row(row_key, [column])
                    except Exception as e:
                        column_dict = None
                    if column_dict:
                        old = int(column_dict[column])
                    data[column] = str(old + v)
                else:
                    if not isinstance(v, unicode):
                        data[column] = str(v)
                    else:
                        data[column] = v
            if data:
                table_au_metrics.put(row_key, data)

