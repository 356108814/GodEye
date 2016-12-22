# encoding: utf-8
"""
用户活跃度指标统计
@author Yuriseus
@create 16-11-25 下午6:11
"""
import datetime

from config.enums import *
from db.db_source import DBSource
from service.base import BaseService
from util.date import DateUtil


class UserActiveMetricsService(BaseService):
    def __init__(self):
        super(UserActiveMetricsService, self).__init__()
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

        # 日活DAU
        column_values = []
        dau_count = self.get_dau(date)
        for d in dau_count:
            mid = d['mid']
            column_value = {
                'product': Product.MINIK,
                'terminal': Terminal.MACHINE,
                'mid': mid,
                'date': date,
                'type': UserActiveType.DAU,
                'count': d['count']
            }
            column_values.append(column_value)
        self.batch_insert(column_values)

        # 周活WAU
        date_obj = DateUtil.str2date(date)
        if date_obj.isoweekday() == 1:
            # 周一统计上一周的，会跨月
            wau_count = self.get_wau(date)
            for d in wau_count:
                mid = d['mid']
                column_value = {
                    'product': Product.MINIK,
                    'terminal': Terminal.MACHINE,
                    'mid': mid,
                    'date': date,
                    'type': UserActiveType.WAU,
                    'count': d['count']
                }
                column_values.append(column_value)
            self.batch_insert(column_values)

        # MAU
        if date_obj.day == 1:
            # 月出统计上一月的
            mau_count = self.get_mau(date)
            for d in mau_count:
                mid = d['mid']
                column_value = {
                    'product': Product.MINIK,
                    'terminal': Terminal.MACHINE,
                    'mid': mid,
                    'date': date,
                    'type': UserActiveType.MAU,
                    'count': d['count']
                }
                column_values.append(column_value)
            self.batch_insert(column_values)

        # SAU
        if date_obj.month in [1, 4, 7, 10]:
            pass

        # 关闭数据库连接
        self.minik.close()
        self.dashboard.close()

    def get_dau(self, date):
        """
        获取DAU，按机器
        :param date: yyyy-mm-dd
        :return:
        """
        ym = date.replace('-', '')[0:6]
        start_date = '%s 00:00:00' % date
        end_date = '%s 23:59:59' % date
        sql = "SELECT mid, SUBSTRING(logintime, 1, 10) AS day, count(distinct(uid)) AS count FROM t_data_user_login_%s WHERE logintime >= '%s' AND logintime <= '%s' GROUP BY mid, day" % \
              (ym, start_date, end_date)
        print(sql)
        return self.minik.query(sql)

    def get_wau(self, date):
        """
        获取WAU，按机器
        :param date: yyyy-mm-dd
        :return:
        """
        date = DateUtil.str2date(date)
        start_date = '%s' % DateUtil.get_interval_day_date(date, -7)
        end_date = '%s' % DateUtil.get_interval_day_date(date, -1)
        return self.get_login_user_count(start_date, end_date)

    def get_mau(self, date):
        """
        获取MAU，按机器
        :param date: yyyy-mm-dd
        :return:
        """
        year = int(date[0:4])
        month = int(date[5:7]) - 1
        if month == 1:
            year -= 1
        day = DateUtil.get_last_day_of_month(year, month)
        start_date = '%s-%s-%s' % (year, month, '01')
        end_date = '%s-%s-%s' % (year, month, day)
        return self.get_login_user_count(start_date, end_date)

    def get_login_user_count(self, start_date, end_date):
        """
        获取指定日期内登录的用户数
        :param start_date: yyyy-mm-dd
        :param end_date: yyyy-mm-dd
        :return:
        """
        data_list = []
        start_date = '%s 00:00:00' % start_date
        end_date = '%s 23:59:59' % end_date
        start_ym = start_date.replace('-', '')[0:6]
        end_ym = end_date.replace('-', '')[0:6]
        sql_tpl = "SELECT mid, uid, SUBSTRING(logintime, 1, 10) AS day FROM t_data_user_login_%s WHERE logintime >= '%s' AND logintime <= '%s'"
        rows = []
        count_dict = {}
        if start_ym != end_ym:
            # 跨月需要查出所有记录，然后程序去重
            rows1 = self.minik.query(sql_tpl % (start_ym, start_date, end_date))
            rows2 = self.minik.query(sql_tpl % (end_ym, start_date, end_date))
            rows.extend(rows1)
            rows.extend(rows2)
        else:
            sql = sql_tpl % (end_ym, start_date, end_date)
            print(sql)
            rows = self.minik.query(sql)

        for row in rows:
            mid = row['mid']
            uid = row['uid']
            if mid not in count_dict:
                count_dict[mid] = {}
            uid_dict = count_dict[mid]
            uid_dict[uid] = 1

        for mid, ud in count_dict.items():
            data_list.append({'mid': mid, 'count': len(ud)})
        return data_list

    def batch_insert(self, column_values):
        """
        批量插入到用户数量指标表
        :param column_values:
        :return:
        """
        if column_values:
            prefix = "INSERT IGNORE INTO user_active_metrics (product, terminal, mid, date, type, count) VALUES "
            value_fmt = "(%(product)s, %(terminal)s, %(mid)s, '%(date)s', %(type)s, %(count)s ) "
            values = []
            for value in column_values:
                value = value_fmt % value
                values.append(value)
            sql = prefix + ', '.join(values)
            self.dashboard.execute(sql)

if __name__ == '__main__':
    ucm = UserActiveMetricsService()
    ucm.update_by_date('2016-11-01')
    # ucm.get_wau(DateUtil.str2date('2016-12-05'))
