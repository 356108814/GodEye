# encoding: utf-8
"""

@author Yuriseus
@create 16-11-4 下午12:45
"""
from db.db_mysql import DBMySQL


class DBSource(object):

    @staticmethod
    def minik():
        return DBMySQL(host='121.201.48.58', port=23306, user='data_team_leader', password='5op5Efl#olmVbiwb',
                                db='minik')