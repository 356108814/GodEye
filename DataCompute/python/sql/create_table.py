# encoding: utf-8
"""

@author Yuriseus
@create 16-12-6 下午5:01
"""
from util.date import DateUtil
from db.db_source import DBSource


def create_user_action():
    user_action = DBSource.user_action()
    date_list = DateUtil.get_range_date_list('2016-01-01', '2016-12-31')
    with open('./user_action.sql') as f:
        sql_tpl = f.read()

    for date in date_list:
        date = date.replace('-', '')
        sql = sql_tpl % date
        user_action.execute(sql)

if __name__ == '__main__':
    create_user_action()
