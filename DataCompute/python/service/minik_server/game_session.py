# encoding: utf-8
"""
游戏局服务
@author Yuriseus
@create 16-10-31 下午4:23
"""
import json

from common.hbase_client import HbaseClient
from config.enums import WEEKDAY, UserType
from service.base import BaseService
from util.date import DateUtil
from db.db_source import DBSource

hbase_client = HbaseClient()
table_metrics = hbase_client.table('minik_game_session_metrics')
table_mid_metrics = hbase_client.table('minik_game_session_mid_metrics')


class GameSessionService(BaseService):
    def __init__(self):
        super(GameSessionService, self).__init__()

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
        # 增加start_time
        d['start_time'] = DateUtil.get_interval_second_date(d['end_time'], -d['durtime'])
        # 更新时间戳
        d['timestamp'] = DateUtil.date2timestamp(d['start_time'], '%Y-%m-%d %H:%M:%S')
        # 增加星期
        weekday_int = DateUtil.str2date(d['start_time'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        # 增加日期
        d['date'] = d['start_time'][0:10].replace('-', '')
        # 增加用户类型
        if int(d['cur_player_uid']) == 0:
            d['user_type'] = UserType.ANONYMOUS
            d['uid'] = ''
        else:
            d['user_type'] = UserType.LOGIN
            # "uid_list":[{"uid":"4032672"},{"uid":"2572404"}]
            uid_list = []
            for d_uid in d['uid_list']:
                uid_list.append(d_uid['uid'])
            d['uid'] = ','.join(uid_list)
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'connector.entryHandler.macPlayingModeGameOverV2' and data:
            dur_time = int(data['durtime'])
            if 0 < dur_time < 24*3600:
                return True
        return False

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        rdd.cache()
        self.count_total_metrics(rdd, sql_context)
        self.count_mid_metrics(rdd, sql_context)
        hbase_client.close()

    def count_total_metrics(self, rdd, sql_context):
        """
        统计总局指标
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        # df.printSchema()
        df.registerTempTable("session")

        metrics_dict = {}
        sql = "SELECT date, weekday, user_type, SUM(durtime) AS time, COUNT(*) AS session_count, SUM(total_songs_num) AS song_count, " \
              "SUM(single_songs_num) AS single_song_count " \
              "FROM session GROUP BY date, weekday, user_type"
        dur_df = sql_context.sql(sql)
        for row in dur_df.collect():
            row_dict = row.asDict()
            # 构造需要保存的指标字典
            date = row_dict['date']
            if date not in metrics_dict:
                metrics_dict[date] = {
                    'session_count': 0,
                    'time': 0,
                    'song_count': 0,
                    'single_song_count': 0,
                    'package_song_count': 0,
                    'anonymous_session_count': 0,
                    'anonymous_time': 0,
                    'login_session_count': 0,
                    'login_time': 0,
                }
            md = metrics_dict[date]
            md['date'] = date
            md['weekday'] = row_dict['weekday']
            session_count = row_dict['session_count']
            time = row_dict['time']
            if row_dict['user_type'] == UserType.ANONYMOUS:
                md['anonymous_session_count'] += session_count
                md['anonymous_time'] += time
            else:
                md['login_session_count'] += session_count
                md['login_time'] += time

            md['session_count'] += session_count
            md['time'] += time
            md['song_count'] += row_dict['song_count']
            md['single_song_count'] += row_dict['single_song_count']
            md['package_song_count'] += (row_dict['song_count'] - row_dict['single_song_count'])
        self.update_total_metrics(metrics_dict)

    def update_total_metrics(self, metrics_dict):
        """
        更新局总指标表
        :param metrics_dict: dict字典
        :return:
        """
        for row_key, metrics in metrics_dict.items():
            cf = 'info'
            data = {}
            for k, v in metrics.items():
                column = '%s:%s' % (cf, k)
                if k in ['date', 'weekday']:
                    data[column] = v
                else:
                    old = 0
                    column_dict = table_metrics.row(row_key, [column])
                    if column_dict:
                        old = int(column_dict[column])
                    data[column] = str(old + v)
                    # data[column] = str(table.counter_inc(row_key, column, metrics[k]))
                    # 使用计数器更新值后，值格式为\x00\x00\x00\x00\x01\xB0h\xC8。如果使用put设置了值后再使用计数器，则报错
                    # org.apache.hadoop.hbase.DoNotRetryIOException: org.apache.hadoop.hbase.DoNotRetryIOException: Field is not a long, it's 5 bytes
                    # table.counter_inc(row_key, column, v)
            if data:
                table_metrics.put(row_key, data)

    def count_mid_metrics(self, rdd, sql_context):
        """
        统计机器局指标
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        # df.printSchema()
        df.registerTempTable("session")

        metrics_dict = {}
        sql = "SELECT date, mid, weekday, user_type, SUM(durtime) AS time, COUNT(*) AS session_count, SUM(total_songs_num) AS song_count, " \
              "SUM(single_songs_num) AS single_song_count " \
              "FROM session GROUP BY date, mid, weekday, user_type"
        dur_df = sql_context.sql(sql)
        for row in dur_df.collect():
            row_dict = row.asDict()
            # 构造需要保存的指标字典
            date = row_dict['date']
            row_key = '%(date)s-%(mid)s' % {'date': date, 'mid': str(row_dict['mid']).zfill(6)}
            if row_key not in metrics_dict:
                metrics_dict[row_key] = {
                    'session_count': 0,
                    'time': 0,
                    'song_count': 0,
                    'single_song_count': 0,
                    'package_song_count': 0,
                    'anonymous_session_count': 0,
                    'anonymous_time': 0,
                    'login_session_count': 0,
                    'login_time': 0,
                }
            md = metrics_dict[row_key]
            md['date'] = date
            md['mid'] = str(row_dict['mid'])
            md['weekday'] = row_dict['weekday']
            session_count = row_dict['session_count']
            time = row_dict['time']
            if row_dict['user_type'] == UserType.ANONYMOUS:
                md['anonymous_session_count'] += session_count
                md['anonymous_time'] += time
            else:
                md['login_session_count'] += session_count
                md['login_time'] += time

            md['session_count'] += session_count
            md['time'] += time
            md['song_count'] += row_dict['song_count']
            md['single_song_count'] += row_dict['single_song_count']
            md['package_song_count'] += (row_dict['song_count'] - row_dict['single_song_count'])
        self.update_mid_metrics(metrics_dict)

    def update_mid_metrics(self, metrics_dict):
        """
        更新机器指标表
        :param metrics_dict: dict字典
        :return:
        """
        for row_key, metrics in metrics_dict.items():
            cf = 'info'
            data = {}
            for k, v in metrics.items():
                column = '%s:%s' % (cf, k)
                if k in ['date', 'weekday', 'mid']:
                    data[column] = v
                else:
                    old = 0
                    column_dict = table_mid_metrics.row(row_key, [column])
                    if column_dict:
                        old = int(column_dict[column])
                    data[column] = str(old + v)
            if data:
                table_mid_metrics.put(row_key, data)

    def save_to_mysql(self, rdd):
        """
        保存到mysql
        :param rdd:
        :return:
        """
        def save(d_iter):
            dashboard = DBSource.dashboard()
            column_values = []
            for d in d_iter:
                value = {}
                value['mode_type'] = int(d['modetype'])
                value['start_date'] = d['start_time']
                value['end_date'] = d['end_time']
                value['dur_time'] = int(d['durtime'])
                value['weekday'] = d['weekday']
                value['mid'] = int(d['mid'])
                value['uid'] = d['uid']
                value['sessionid'] = d['sessionid']

                value['industry'] = ''
                value['update_date'] = 'now()'

                column_values.append(value)
            self.insert_to_mysql_by_dict_list(dashboard, 'minik_game_session', column_values)
        rdd.foreachPartition(save)






