# encoding: utf-8
"""
基础业务类
@author Yuriseus
@create 2016-8-3 20:10
"""
import settings
from coffeebean.db_mysql import DBMySQL
from coffeebean.log import logger
from coffeebean.cache import Cache


class BaseService(object):
    def __init__(self, table_name):
        self.table_name = table_name
        self.logger = logger
        self.cache = Cache.current()
        conf = settings.CONF['db']
        self.db = DBMySQL(conf['host'], conf['port'], conf['user'], conf['password'], conf['db'])

    def get(self, pk_id):
        """
        根据主键获取记录
        @param pk_id:
        @return:
        """
        sql = "SELECT * FROM " + self.table_name + " WHERE id = %(id)s"
        return self.db.query(sql, {'id': pk_id}, True)

    def save(self, model_dict):
        """
        保存到数据表
        @param model_dict: 模型数据字典，注意：字典键名称即表列名
        @return:
        """
        sql_tpl = "INSERT INTO " + self.table_name + " (%(columns)s) VALUES (%(values)s)"
        c_l = []
        v_l = []
        for k, v in model_dict.items():
            c_l.append(k)
            v_l.append('%(' + k + ')s')
        sql = (sql_tpl % {'columns': ', '.join(c_l), 'values': ', '.join(v_l)})
        is_success = self.db.execute(sql, model_dict)
        return is_success

    def update(self, model_dict):
        """
        更新数据表
        @param model_dict: 模型数据字典，注意：字典键名称即表列名。id为主键名
        @return:
        """
        sql_tpl = "UPDATE " + self.table_name + " SET %(new_column_value)s) WHERE id = %(id)s"
        column_value = []
        for k, v in model_dict.items():
            column_value.append('%s = %s' % ('%(' + k + ')s', v))
        sql = (sql_tpl % {'new_column_value': ', '.join(column_value), 'id': model_dict['id']})
        is_success = self.db.execute(sql, model_dict)
        return is_success

    def get_all(self, exclude_columns=None):
        """
        获取所有记录
        @param exclude_columns: 需要排除的列
        @return:
        """
        sql = "select * FROM %s " % self.table_name
        rows = self.db.query(sql)
        for row in rows:
            if exclude_columns:
                for c in exclude_columns:
                    del row[c]
        return rows


