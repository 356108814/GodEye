# encoding: utf-8
"""
db数据源
@author Yuriseus
@create 16-11-15 上午10:16
"""

from db.db_mysql import DBMySQL
import config.settings as settings


class DBSource(object):
    def __init__(self):
        pass

    @staticmethod
    def dashboard():
        if not settings.DEBUG:
            return DBMySQL(host='172.16.2.101', port=3307, user='root', password='root',
                           db='aimei_data_dashboard')
        else:
            return DBMySQL(host='172.16.1.189', port=3306, user='root', password='root',
                           db='aimei_data_dashboard')

    @staticmethod
    def user_action():
        if not settings.DEBUG:
            return DBMySQL(host='172.16.2.101', port=3307, user='root', password='root',
                           db='aimei_data_useraction')
        else:
            return DBMySQL(host='172.16.1.189', port=3306, user='root', password='root',
                           db='aimei_data_useraction')

    @staticmethod
    def minik():
        return DBMySQL(host='121.201.48.58', port=23306, user='root', password='root',
                       db='minik')

    @staticmethod
    def minik_song():
        return DBMySQL(host='172.16.2.100', port=3306, user='root', password='root',
                       db='minik_song_db')

    @staticmethod
    def kshow_song():
        return DBMySQL(host='120.132.0.183', port=3308, user='root', password='root',
                       db='kshow_song_db')
