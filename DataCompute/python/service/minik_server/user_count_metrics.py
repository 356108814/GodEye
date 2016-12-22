# encoding: utf-8
"""
用户数量指标统计
@author Yuriseus
@create 16-11-25 下午6:11
"""
import datetime

from config.enums import *
from db.db_source import DBSource
from service.base import BaseService
from util.date import DateUtil


class UserCountMetricsService(BaseService):
    def __init__(self):
        super(UserCountMetricsService, self).__init__()
        self.minik = DBSource.minik()
        self.dashboard = DBSource.dashboard()

    def update_by_date(self, date=None):
        """
        默认统计前一天的
        :param date: yyyy-mm-dd
        :return:
        """
        if not date:
            yesterday = DateUtil.get_interval_day_date(datetime.datetime.now(), -1)
            date = yesterday

        # 新增
        column_values = []
        subscribe_count = self.get_subscribe_count(date)
        for d in subscribe_count:
            hour = d['hour'][11:13]
            mid = d['mid']
            if mid >= 100000:    # 4294967295
                mid = 0
            column_value = {
                'product': Product.MINIK,
                'terminal': Terminal.MACHINE,
                'mid': mid,
                'date': date,
                'hour': hour,
                'type': UserCountType.ADD,
                'count': d['count']
            }
            column_values.append(column_value)
        self.batch_insert(column_values)

        # 流失
        column_values = []
        unsubscribe_count = self.get_unsubscribe_count(date)
        for d in unsubscribe_count:
            hour = d['hour'][11:13]
            mid = d['mid']
            if mid >= 100000:  # 4294967295
                mid = 0
            column_value = {
                'product': Product.MINIK,
                'terminal': Terminal.MACHINE,
                'mid': mid,
                'date': date,
                'hour': hour,
                'type': UserCountType.LOST,
                'count': d['count']
            }
            column_values.append(column_value)
        self.batch_insert(column_values)

        # 总计
        accumulative_count = self.get_accumulative_count(date)
        column_value = {
            'product': Product.MINIK,
            'terminal': Terminal.MACHINE,
            'mid': 0,
            'date': date,
            'hour': '23',
            'type': UserCountType.TOTAL,
            'count': accumulative_count
        }
        self.batch_insert([column_value])
        # 关闭数据库连接
        self.minik.close()
        self.dashboard.close()

    def get_subscribe_count(self, date):
        """
        获取新订阅用户数，按机器和小时维度统计
        :param date: yyyy-mm-dd
        :return:
        """
        start_timestamp = DateUtil.date2timestamp('%s 00:00:00' % date, '%Y-%m-%d %H:%M:%S')
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT reg_mid AS mid, SUBSTRING(FROM_UNIXTIME(subscribeTime), 1, 13) AS hour, count(*) AS count FROM t_baseinfo_user WHERE reg_mid != 0 AND subscribeTime >= %s AND subscribeTime <= %s GROUP BY mid, hour" % \
              (start_timestamp, end_timestamp)
        return self.minik.query(sql)

    def get_unsubscribe_count(self, date):
        """
        获取取消订阅用户数，按机器和小时维度统计
        :param date: yyyy-mm-dd
        :return:
        """
        start_timestamp = DateUtil.date2timestamp('%s 00:00:00' % date, '%Y-%m-%d %H:%M:%S')
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT reg_mid AS mid, SUBSTRING(FROM_UNIXTIME(unsubscribeTime), 1, 13) AS hour, count(*) AS count FROM t_baseinfo_user WHERE reg_mid != 0 AND unsubscribeTime >= %s AND unsubscribeTime <= %s GROUP BY mid, hour" % \
              (start_timestamp, end_timestamp)
        return self.minik.query(sql)

    def get_accumulative_count(self, date):
        """
        获取累计用户数
        :param date:
        :return:
        """
        # start_timestamp = DateUtil.date2timestamp('%s 00:00:00' % date, '%Y-%m-%d %H:%M:%S')
        end_timestamp = DateUtil.date2timestamp('%s 23:59:59' % date, '%Y-%m-%d %H:%M:%S')
        sql = "SELECT count(*) AS count FROM t_baseinfo_user WHERE subscribeTime <= %s" % end_timestamp
        count = int(self.minik.query(sql)[0]['count'])
        return count

    def batch_insert(self, column_values):
        """
        批量插入到用户数量指标表
        :param column_values:
        :return:
        """
        if column_values:
            prefix = "INSERT IGNORE INTO user_count_metrics (product, terminal, mid, date, hour, type, count) VALUES "
            value_fmt = "(%(product)s, %(terminal)s, %(mid)s, '%(date)s', '%(hour)s', %(type)s, %(count)s ) "
            values = []
            for value in column_values:
                value = value_fmt % value
                values.append(value)
            sql = prefix + ', '.join(values)
            self.dashboard.execute(sql)

if __name__ == '__main__':
    ucm = UserCountMetricsService()
    ucm.update_by_date('2016-11-27')
