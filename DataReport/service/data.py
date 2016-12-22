# encoding: utf-8
"""
aimei_data数据库查询服务
@author Yuriseus
@create 2016-12-8 14:49
"""
from service.base import BaseService
from config.enums import *
from util.date import DateUtil


class DataService(BaseService):

    def __init__(self):
        super().__init__()

    def get_income_by_date(self, date):
        """
        按天获取机器收入
        :param date: yyyy-mm-dd
        :return dict: 单位分
        """
        income = {
            'date': date,
            'total': 0,
            'mobile_pay': 0,
            'coin': 0,
            'single': 0,
            'time15': 0,
            'time30': 0,
            'time60': 0,
            'songs3': 0,
            'songs7': 0,
            'songs10': 0
        }
        y_m = date.replace('-', '')[0:6]
        day = date[8:10]
        start_time_span = int(day + '00')
        end_time_span = int(day + '23')
        sql = 'SELECT m_type, pkg_id, SUM(money_count) AS money FROM t_data_aimei_consum_mode_mid_detail_total_%s WHERE ' \
              'time_span >= %s AND time_span <= %s GROUP BY m_type, pkg_id' % (y_m, start_time_span, end_time_span)
        rows = self.db_data.query(sql)
        for row in rows:
            m_type = int(row['m_type'])
            pkg_id = int(row['pkg_id'])
            money = int(row['money'])

            income['total'] += money
            if m_type == Money.INSERT_COIN:
                income['coin'] += money
            elif m_type in [Money.ALIPAY, Money.WXPAY]:
                income['mobile_pay'] += money

            if pkg_id == PackageType.SINGLE:
                income['single'] += money
            elif pkg_id == PackageType.TIME15:
                income['time15'] += money
            elif pkg_id == PackageType.TIME30:
                income['time30'] += money
            elif pkg_id == PackageType.TIME60:
                income['time60'] += money
            elif pkg_id == PackageType.SONGS3:
                income['songs3'] += money
            elif pkg_id == PackageType.SONGS7:
                income['songs7'] += money
            elif pkg_id == PackageType.SONGS10:
                income['songs10'] += money
        return income

    def get_income_by_date_range(self, start_date, end_date):
        """
        根据日期范围获取机器收入
        :param start_date: yyyy-mm-dd
        :param end_date: yyyy-mm-dd
        :return list: income list 金钱单位：元
        """
        incomes = []
        date_list = DateUtil.get_range_date_list(start_date, end_date)
        for date in date_list:
            income = self.get_income_by_date(date)
            for k, v in income.items():
                if k != 'date':
                    income[k] = v/100.0
            incomes.append(income)
        return incomes


if __name__ == '__main__':
    s = DataService()
    income = s.get_income_by_date('2016-12-07')
    print(income)
    print(income['coin'] + income['mobile_pay'])

