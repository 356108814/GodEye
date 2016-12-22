# encoding: utf-8
"""
用户行为
@author Yuriseus
@create 2016-8-17 15:14
"""
import time

from config import settings
from config.enums import Action
from config.enums import Product
from config.enums import Terminal
from util.date import DateUtil
from ..base_file import BaseFileService


class UserActionService(BaseFileService):

    def __init__(self, dir_path):
        # if not dir_path:
        #     class_path = __name__ + '.' + self.__class__.__name__
        super().__init__(dir_path)
        self.batch_lines_count = 10000
        self._t1 = 0
        self._t_index = 0

    def is_need_drop(self, line_data):
        if line_data.find('uid') == -1:
            return True
        return False

    def get_clean_data(self, line_data):
        return line_data

    def process(self, lines):
        # 格式：[I 160815 00:00:00 record_handler:181] timestamp=2016-08-15_00:00:00&action=click_listen_song&songid=2306797&uid=1277909
        if self._t_index == 0:
            self._t1 = time.time()
        column_values = []
        for line in lines:
            column_value = self.get_column_value(line)
            column_values.append(column_value)

        self.insert_many(column_values)
        self.insert_total_many(column_values)

        if settings.DEBUG:
            self._t_index += self.batch_lines_count
            if self._t_index == 280000:
                print('total time: %s' % (time.time() - self._t1))

    def get_column_value(self, line):
        """
        根据行数据获取需要插入到数据库的字典
        :param line:
        :return:
        """
        index = line.find(']')
        line = (line[index+1:]).strip()
        data = self.parser.split_k_v(line)
        column_value = {
            'uid': data['uid'],
            'p_type': Product.MINIK.value,
            't_type': Terminal.Weixin.value,
            'a_type': -1,
            'action_time': -1,
            'location': -1,
            'aimei_object': '',
            'update_time': -1
        }
        action = data['action']
        if action == 'click_listen_song':
            column_value['a_type'] = Action.REQUEST_SONG.value
            column_value['aimei_object'] = data['songid']
        else:
            column_value['a_type'] = Action.CLICK_BUTTON.value
            aimei_object = ''
            if action == 'click_tab':
                aimei_object = data['tab']
            elif action == 'visit_url':
                aimei_object = data['url']
            elif action == 'click_play_album':
                aimei_object = data['albumid']
            column_value['aimei_object'] = aimei_object
        column_value['action_time'] = DateUtil.date2timestamp(data['timestamp'], fmt='%Y-%m-%d_%H:%M:%S')

        month = DateUtil.now('%Y%m')
        column_value['month'] = month
        column_value['update_time'] = DateUtil.now('%Y-%m-%d %H:%M:%S')

        # 流水需要的

        # 总表需要的
        column_value['provinceid'] = -1
        column_value['isp'] = -1
        column_value['count'] = 1
        column_value['time_span'] = self.get_time_span(data['timestamp'])
        return column_value

    def insert_many(self, column_values):
        month = DateUtil.now('%Y%m')
        prefix = "INSERT INTO t_data_aimei_user_action_%(month)s (uid, p_type, t_type, a_type, action_time, location, aimei_object, update_time) VALUES "
        prefix = prefix % {'month': month}
        value_fmt = "(%(uid)s, '%(p_type)s', '%(t_type)s', '%(a_type)s', %(action_time)s, %(location)s, '%(aimei_object)s', '%(update_time)s')"
        values = []
        for value in column_values:
            value = value_fmt % value
            values.append(value)
        sql = prefix + ', '.join(values)
        self.db_data.execute(sql)

    def insert_total_many(self, column_values):
        month = DateUtil.now('%Y%m')
        prefix = "INSERT INTO t_data_aimei_user_action_mob_%(month)s (p_type, a_type, provinceid, isp, time_span, aimei_object, count, update_time) VALUES "
        prefix = prefix % {'month': month}
        value_fmt = "('%(p_type)s', '%(a_type)s', %(provinceid)s, %(isp)s, %(time_span)s, '%(aimei_object)s', %(count)s, '%(update_time)s') "
        end = "ON DUPLICATE KEY UPDATE count = count + 1, update_time = '%(update_time)s'"
        end = end % {'update_time': DateUtil.now('%Y-%m-%d %H:%M:%S')}
        values = []
        for value in column_values:
            value = value_fmt % value
            values.append(value)
        sql = prefix + ', '.join(values) + end
        self.db_data.execute(sql)

    def insert(self, column_value, data=None):
        """
        插入流水表
        :param column_value:
        """
        # 设置默认值
        month = DateUtil.now('%Y%m')
        column_value['month'] = month
        column_value['update_time'] = DateUtil.now('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO t_data_aimei_user_action_%(month)s (uid, p_type, t_type, a_type, action_time, location, aimei_object, update_time) VALUES (%(uid)s, '%(p_type)s', '%(t_type)s', '%(a_type)s', %(action_time)s, %(location)s, '%(aimei_object)s', '%(update_time)s')"
        sql %= column_value    # 格式化
        # self.db.execute(sql)
        return sql, column_value

    def insert_or_update_total(self, column_value, data=None):
        """
        插入或更新流水总表
        :param column_value:
        """
        # 设置默认值
        month = DateUtil.now('%Y%m')
        column_value['month'] = month
        column_value['provinceid'] = -1
        column_value['isp'] = -1
        column_value['count'] = 1
        column_value['time_span'] = self.get_time_span(data['timestamp'])
        column_value['update_time'] = DateUtil.now('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO t_data_aimei_user_action_mob_%(month)s (p_type, a_type, provinceid, isp, time_span, aimei_object, count, update_time) VALUES ('%(p_type)s', '%(a_type)s', %(provinceid)s, %(isp)s, %(time_span)s, '%(aimei_object)s', %(count)s, '%(update_time)s') ON DUPLICATE KEY UPDATE count = count + 1, update_time = '%(update_time)s'"
        sql %= column_value    # 格式化
        # self.db.execute(sql)
        return sql, column_value

    def get_time_span(self, date_str):
        # 2016-08-15_23:59:59
        time_span = date_str[8:10] + date_str[11:13]
        return int(time_span)
