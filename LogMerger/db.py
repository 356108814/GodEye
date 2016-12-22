# encoding: utf-8
"""
数据库操作
@author Yuriseus
@create 2016-10-9 16:53
"""
import os
import sqlite3

SHOW_SQL = True


class DBSqlite(object):
    def __init__(self, path):
        self.path = path
        self.conn = self.get_conn(path)
        self.conn.row_factory = DBSqlite.dict_factory
        self.cursor = self.conn.cursor()

    @staticmethod
    def get_conn(path):
        if os.path.exists(path) and os.path.isfile(path):
            return sqlite3.connect(path)
        else:
            return sqlite3.connect(':memory:')

    def get_cursor(self):
        if self.conn is not None:
            return self.conn.cursor()
        else:
            return self.get_conn('').cursor()

    @staticmethod
    def dict_factory(cursor, row):
        return dict((col[0], row[idx]) for idx, col in enumerate(cursor.description))

    def save(self, sql, data=None):
        try:
            if sql:
                if data:
                    for d in data:
                        if SHOW_SQL:
                            print('执行sql:[{}],参数:[{}]'.format(sql, d))
                        self.cursor.execute(sql, d)
                else:
                    if SHOW_SQL:
                        print(sql)
                    self.cursor.execute(sql)
                self.conn.commit()
        except Exception as e:
            pass

    def fetchall(self, sql):
        result = None
        if sql:
            if SHOW_SQL:
                print('执行sql:[{}]'.format(sql))
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        return result

    def fetchone(self, sql, param=None):
        result = None
        if sql:
            if SHOW_SQL:
                print('执行sql:[{}],参数:[{}]'.format(sql, param))
            if param:
                self.cursor.execute(sql, param)
            else:
                self.cursor.execute(sql)
            result = self.cursor.fetchone()
        return result

    def desc_table(self, table_name):
        data = {}
        sql = 'PRAGMA table_info(%s)' % table_name
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        for index, row in enumerate(result):
            data[index] = row[1]
        return data

    def update(self, sql, data):
        if sql and data:
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                self.cursor.execute(sql, d)
            self.conn.commit()

    def delete(self, sql, data):
        if sql and data:
            if SHOW_SQL:
                print('执行sql:[{}],参数:[{}]'.format(sql, data))
            self.cursor.execute(sql, data)
            self.conn.commit()

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            pass

if __name__ == '__main__':
    db = DBSqlite('J:\\meda\\data_aimei\\GodEye\\LogMerger\\since.db')
    # print(db.desc_table('record'))
    # print(db.fetchone('select * from record'))
    print(db.fetchall('select * from record'))

