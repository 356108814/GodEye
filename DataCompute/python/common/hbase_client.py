# encoding: utf-8
"""
hbase 客户端，基于happybase
@author Yuriseus
@create 16-11-3 下午2:39
"""

import happybase

import config.settings


class HbaseClient(object):
    def __init__(self, host=None):
        if host:
            self.host = host
        else:
            self.host = '172.16.1.189'
            if not config.settings.DEBUG:
                self.host = 'CDH-1'
        self.connection = None

    def get_connection(self):
        return happybase.Connection(self.host)

    def put(self, table_name, row_key, data):
        """
        更新表
        :param table_name:
        :param row_key:
        :param dict data: dict 键为列名
        :return:
        """
        table = self.connection.table(table_name)
        table.put(row_key, data)

    def get(self, table_name, row_key, columns):
        """
        获取值
        :param table_name:
        :param row_key:
        :param list_or_tuple columns: 列名列表
        :return: dict
        """
        table = self.connection.table(table_name)
        row = table.row(row_key, columns)
        return row

    def table(self, table_name):
        """
        获取表操作对象
        :param table_name:
        :return:
        """
        self.connection = self.get_connection()
        return self.connection.table(table_name)

    def close(self):
        self.connection.close()
