# encoding: utf-8
"""
minik消费服务
@author Yuriseus
@create 16-11-11 下午3:17
"""
import json

from common.hbase_client import HbaseClient
from config.enums import WEEKDAY, Product
from service.base import BaseService
from util.date import DateUtil


class ConsumeService(BaseService):
    def __init__(self):
        super(ConsumeService, self).__init__()
        self.hbase_client = HbaseClient()

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            d['data'] = json.loads(tmp[3])  # 字典
        except Exception as e:
            d['data'] = None
        d['exec_version'] = tmp[4]  # 程序版本
        d['ui_version'] = tmp[5]  # UI版本
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
        # 增加的
        if 'uid' not in data:
            d['uid'] = 0
        timestamp = int(d['timestamp'])
        d['date'] = DateUtil.timestamp2date(timestamp, '%Y-%m-%d %H:%M:%S')
        d['hour'] = d['date'][0:13]  # 用于按小时统计
        weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        d['isoweekday'] = weekday_int
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'connector.entryHandler.accountRecordInfo' and data:
            return True
        return False

    def save_to_consume(self, rdd):
        """
        保存明细信息
        :param rdd:
        :return:
        """

        def save(data_iter):
            table = self.hbase_client.table('minik_consume')
            table2 = self.hbase_client.table('minik_user_consume')
            save_data = {}
            for data in data_iter:
                # 构造需要保存的指标字典
                mid = str(data['mid']).zfill(6)
                uid = str(data['uid']).zfill(8)
                auto_idx = str(data['auto_idx']).zfill(8)
                consume_type = data['type']
                # mid(6位，不足前面补0)-type-timestamp-auto_idx(8位，不足前面补0)
                row_key = '%(mid)s-%(consume_type)s-%(timestamp)s-%(auto_idx)s' % {'mid': mid,
                                                                                   'consume_type': consume_type,
                                                                                   'timestamp': data['timestamp'],
                                                                                   'auto_idx': auto_idx}
                # uid(8位，不足前面补0)-timestamp-auto_idx(8位，不足前面补0)
                row_key2 = '%(uid)s-%(timestamp)s-%(auto_idx)s' % {'uid': uid, 'timestamp': data['timestamp'],
                                                                   'auto_idx': auto_idx}

                for k, v in data.items():
                    if k in ['timestamp', 'hour']:
                        continue
                    save_data[k] = v
                self.update_by_dict(table, {row_key: save_data}, [])

                # 对用户来说，只记录消费的
                if consume_type == 1:
                    self.update_by_dict(table2, {row_key2: save_data}, [])
            self.hbase_client.close()

        rdd.foreachPartition(save)

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        self.count_consume_metrics(rdd, sql_context)
        self.count_consume_mid_metrics(rdd, sql_context)

    def count_consume_metrics(self, rdd, sql_context):
        """
        按日期统计总指标
        :param rdd:
        :param sql_context:
        :return:
        """
        table_name = 'consume_metrics'
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable(table_name)

        sql = "SELECT hour, SUM(coin*coin_price) AS coin, SUM(mobile_pay) AS mobile_pay FROM %s " \
              "WHERE type = 1 GROUP BY hour" % table_name
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                coin = row_dict['coin']/100.0
                mobile_pay = row_dict['mobile_pay']
                # product-type-yyyymmdd
                row_key = '%(product)s-%(type)s-%(date)s' % {'product': Product.MINIK, 'type': 'day',
                                                             'date': date.replace('-', '')}
                if row_key not in metrics_dict:
                    metrics_dict[row_key] = {
                        'coin': 0,
                        'mobile_pay': 0
                    }
                md = metrics_dict[row_key]
                md['date'] = date
                weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
                md['weekday'] = WEEKDAY[weekday_int]
                md['isoweekday'] = weekday_int
                md['ch%s' % hour] = coin
                md['mh%s' % hour] = mobile_pay
                md['coin'] += coin
                md['mobile_pay'] += mobile_pay
            table = self.hbase_client.table(table_name)
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def count_consume_mid_metrics(self, rdd, sql_context):
        """
        按日期和机器统计总指标
        :param rdd:
        :param sql_context:
        :return:
        """
        table_name = 'consume_mid_metrics'
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable(table_name)

        sql = "SELECT mid, hour, SUM(coin*coin_price) AS coin, SUM(mobile_pay) AS mobile_pay FROM %s " \
              "WHERE type = 1 GROUP BY mid, hour" % table_name
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                mid = str(row_dict['mid']).zfill(6)
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                coin = row_dict['coin']/100.0
                mobile_pay = row_dict['mobile_pay']
                # product-type-mid(6位，不足前面补0)-yyyymmdd
                row_key = '%(product)s-%(type)s-%(mid)s-%(date)s' % {'product': Product.MINIK, 'type': 'day',
                                                                     'mid': mid, 'date': date.replace('-', '')}
                if row_key not in metrics_dict:
                    metrics_dict[row_key] = {
                        'coin': 0,
                        'mobile_pay': 0
                    }
                md = metrics_dict[row_key]
                md['date'] = date
                weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
                md['weekday'] = WEEKDAY[weekday_int]
                md['isoweekday'] = weekday_int
                md['ch%s' % hour] = coin
                md['mh%s' % hour] = mobile_pay
                md['coin'] += coin
                md['mobile_pay'] += mobile_pay
            table = self.hbase_client.table(table_name)
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def get_increment_keys(self):
        increment_keys = ['coin', 'mobile_pay']
        for x in range(24):
            increment_keys.append('%s%s' % ('c', x))
            increment_keys.append('%s%s' % ('m', x))
        return increment_keys
