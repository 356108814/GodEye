# encoding: utf-8
"""
合并记录处理
@author Yuriseus
@create 2016-10-9 18:02
"""
import time

import datetime

import setting
from date_util import DateUtil
from db import DBSqlite


class SinceUtil(object):

    def __init__(self):
        if setting.DEBUG:
            self.db = DBSqlite('J:\\meda\\data_aimei\\GodEye\\LogMerger\\since.db')
        else:
            self.db = DBSqlite('/diskb/RTCS/LogMerger/since.db')

    def add_2_db(self, file_path, log_type):
        sql_tpl = 'INSERT INTO "main"."record" ("file_path", "line", "type", "create_timestamp", "update_timestamp") VALUES (\'%s\', 0, \'%s\', %s, \'\')'
        now = DateUtil.now_timestamp()
        sql = sql_tpl % (file_path, log_type, now)
        self.db.save(sql)

    def update_2_db(self, file_path, line):
        """
        更新文件处理所在行数
        :param file_path:
        :param line:
        :return:
        """
        sql_tpl = 'UPDATE "main"."record" SET line = %s, update_timestamp = %s WHERE file_path = \'%s\' '
        now = DateUtil.now_timestamp()
        sql = sql_tpl % (line, now, file_path)
        self.db.save(sql)

    def get_records_by_status(self, status):
        """
        获取所有已合并的记录
        :param status: 0未合并完成，1已合并完成
        :return:
        """
        records = {}
        sql = 'SELECT * FROM record WHERE status = %s' % status
        result = self.db.fetchall(sql)
        for r in result:
            records[r['file_path']] = r['line']
        return records

    def update_2_complete(self, file_path):
        """
        设置文件为处理完成状态
        :param file_path:
        :return:
        """
        sql_tpl = 'UPDATE "main"."record" SET status = %s, update_timestamp = %s WHERE file_path = \'%s\' '
        now = DateUtil.now_timestamp()
        sql = sql_tpl % (1, now, file_path)
        self.db.save(sql)

    def clear_history(self):
        """
        清空一周前的历史日志
        :return:
        """
        date = DateUtil.get_interval_day_date(datetime.datetime.now(), -7)
        timestamp = DateUtil.date2timestamp(date)
        sql = 'DELETE FROM record WHERE create_timestamp <= :create_timestamp'
        self.db.delete(sql, {'create_timestamp': timestamp})

if __name__ == '__main__':
    util = SinceUtil()
    # util.add_2_db('/data/12.log', 'test_log')
    # util.update_2_db('/data/11.log', 10)
    util.clear_history()
