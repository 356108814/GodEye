# encoding: utf-8
"""
基础解析服务
@author Yuriseus
@create 2016-8-25 15:43
"""
from config import settings
from db.db_mysql import DBMySQL
from util.log import logger
from util.parser import ParserUtil


class BaseService(object):
    def __init__(self):
        self.parser = ParserUtil
        # db_conf = settings.CONF['db_data']
        # db_static_conf = settings.CONF['db_static_center']
        # db_useraction_conf = settings.CONF['db_useraction']
        # self.db_data = DBMySQL(db_conf['host'], db_conf['port'], db_conf['user'], db_conf['password'], db_conf['db'])
        # self.db_static_center = DBMySQL(db_static_conf['host'], db_static_conf['port'], db_static_conf['user'], db_static_conf['password'], db_static_conf['db'])
        # self.db_useraction = DBMySQL(db_useraction_conf['host'], db_useraction_conf['port'], db_useraction_conf['user'], db_useraction_conf['password'], db_useraction_conf['db'])
        self.log = logger

    def print_schema(self, data_frame):
        if settings.DEBUG:
            if data_frame:
                data_frame.printSchema()

    def update_by_dict(self, table, d, increment_keys):
        """
        根据字典更新，键为row_key
        :param table: happybase table 实例
        :param d: dict字典
        :param increment_keys: 增长的column
        :return:
        """
        for row_key, metrics in d.items():
            cf = 'info'
            data = {}
            for k, v in metrics.items():
                column = '%s:%s' % (cf, k)
                if k in increment_keys:
                    old = 0
                    try:
                        column_dict = table.row(row_key, [column])
                    except Exception as e:
                        column_dict = None
                    if column_dict:
                        ov = str(column_dict[column])
                        if ov.find('.') != -1:
                            old = float(ov)
                        else:
                            old = int(ov)
                    data[column] = str(old + v)
                else:
                    if not isinstance(v, unicode) or isinstance(v, int) or isinstance(v, float):
                        data[column] = str(v)
                    else:
                        data[column] = v
            if data:
                table.put(row_key, data)

    def insert_to_mysql_by_dict_list(self, db, table_name, column_values):
        """
        批量插入到mysql
        :param db:
        :param table_name:
        :param column_values:
        :return:
        """
        is_success = True
        if db and table_name and column_values:
            first = column_values[0]
            keys = first.keys()
            param_name = ','.join(first.keys())
            value_fmts = []
            for name in keys:
                value = first[name]
                if isinstance(value, str) or isinstance(value, unicode):
                    if value == 'now()':
                        value_fmts.append("%(" + name + ")s")
                    else:
                        value_fmts.append("'%(" + name + ")s'")
                else:
                    value_fmts.append("%(" + name + ")s")
            param_value = ','.join(value_fmts)
            prefix = "INSERT INTO %(table_name)s (%(param_name)s) VALUES " % {'table_name': table_name, 'param_name': param_name}
            value_fmt = " (%(param_value)s) " % {'param_value': param_value}
            values = []
            for value in column_values:
                value = value_fmt % value
                values.append(value)
            if values:
                sql = prefix + ', '.join(values)
                affected_row_num = db.execute(sql)
                if not affected_row_num:
                    is_success = False
        return is_success

    def insert_or_update_to_mysql_by_dict_list(self, db, table_name, column_values):
        """
        批量插入到mysql。若唯一键约束了，则更新count
        :param db:
        :param table_name:
        :param column_values:
        :return:
        """
        is_success = True
        if db and table_name and column_values:
            first = column_values[0]
            keys = first.keys()
            param_name = ','.join(first.keys())
            value_fmts = []
            for name in keys:
                value = first[name]
                if isinstance(value, str) or isinstance(value, unicode):
                    if value == 'now()':
                        value_fmts.append("%(" + name + ")s")
                    else:
                        value_fmts.append("'%(" + name + ")s'")
                else:
                    value_fmts.append("%(" + name + ")s")
            param_value = ','.join(value_fmts)
            prefix = "INSERT INTO %(table_name)s (%(param_name)s) VALUES " % {'table_name': table_name, 'param_name': param_name}
            end = " ON DUPLICATE KEY UPDATE count = count + VALUES(count), update_date = now() "
            value_fmt = " (%(param_value)s) " % {'param_value': param_value}
            values = []
            for value in column_values:
                value = value_fmt % value
                values.append(value)
            if values:
                sql = prefix + ', '.join(values) + end
                affected_row_num = db.execute(sql)

                if not affected_row_num:
                    is_success = False
        return is_success
