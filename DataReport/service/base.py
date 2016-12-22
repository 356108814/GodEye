# encoding: utf-8
"""
服务基类
@author Yuriseus
@create 2016-12-8 14:41
"""
from db.db_source import DBSource
from util.date import DateUtil
from util.log import logger


class BaseService(object):

    def __init__(self):
        self.db_data = DBSource.data()
        self.db_dashboard = DBSource.dashboard()
        self.logger = logger

    def get_by_date_range(self, start_date, end_date, fun):
        """
        根据日期范围获取
        :param end_date:
        :param start_date:
        :param fun: 获取一天的函数
        :return:
        """
        data = []
        date_list = DateUtil.get_range_date_list(start_date, end_date)
        for date in date_list:
            data.append(fun(date))
        return data

