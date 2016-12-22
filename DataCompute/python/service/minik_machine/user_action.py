# encoding: utf-8
"""
用户行为spark服务
@author Yuriseus
@create 16-11-08 下午17:29
"""
import json

from common.hbase_client import HbaseClient
from config.enums import *
from db.db_source import DBSource
from service.base import BaseService
from util.date import DateUtil
from util.parser import ParserUtil


class UserActionService(BaseService):
    def __init__(self):
        super(UserActionService, self).__init__()
        self.hbase_client = HbaseClient()

    def split(self, line):
        """
        获取清洗过的行数据。清洗过程：转换、补全数据
        :param line:
        :return:
        """
        # 格式：timestamp=16-09-04_14:54:52&page=play&remain_coin=27&session_id=18418&remain_time=1881&remain_songs=0&uid=2090466&sequence_id=221&mid=230&current_coin_value=100&cur_package_id=-1&ui_version=2.0.0.28&exec_version=2.0.1.8&action=cf_promate_detail&sub_page=main_menu&period_id=95
        line_data = line.replace('\n', '')
        if line_data.find('action=cf_song_listen&sub_page=system_logou') != -1:  # 补全song_id
            tmp = line_data.split('&')
            tmp[len(tmp) - 1] = 'song_id=%s' % tmp[len(tmp) - 1]
            line_data = '&'.join(tmp)
        data = ParserUtil.split_k_v(line_data)
        if 'timestamp' not in data:
            data['timestamp'] = -1
        else:
            data['timestamp'] = DateUtil.date2timestamp(data['timestamp'], fmt='%y-%m-%d_%H:%M:%S')
        if 'action' not in data:
            data['action'] = None
        return data

    def process_data(self, d):
        # 增加
        date = DateUtil.timestamp2date(d['timestamp'], fmt='%Y-%m-%d %H:%M:%S')
        d['date'] = date
        d['hour'] = date[0:13]  # 用于按小时统计
        d['day'] = date[0:10]  # 用于按天统计
        if d['uid'] == 0:
            d['user_type'] = UserType.ANONYMOUS
        else:
            d['user_type'] = UserType.LOGIN
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        timestamp = d['timestamp']
        if d['timestamp'] == -1 or d['action'] is None or d['action'] in ['single_game_static'] \
                or timestamp <= 1451577600 or timestamp >= 1514736000:
            return False
        return True

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        # self.count_metrics(rdd, sql_context)
        self.count_click_metrics(rdd, sql_context)

    def count_metrics(self, rdd, sql_context):
        """
        按日期统计点击数
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("user_action")

        sql = "SELECT user_type, action, hour, COUNT(*) AS count FROM user_action GROUP BY user_type, action, hour"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                count = row_dict['count']
                action = row_dict['action']
                # usertype-type-action-yyyymmdd
                row_key = '%(user_type)s-%(type)s-%(action)s-%(date)s' % {'user_type': row_dict['user_type'],
                                                                          'type': 'day',
                                                                          'action': action,
                                                                          'date': date.replace('-', '')}
                if row_key not in metrics_dict:
                    metrics_dict[row_key] = {
                        'count': 0
                    }
                md = metrics_dict[row_key]
                md['date'] = date
                weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
                md['weekday'] = WEEKDAY[weekday_int]
                md['h%s' % hour] = count
                md['action'] = action
                md['count'] += count
            table = self.hbase_client.table('minik_user_action_metrics')
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def get_increment_keys(self):
        increment_keys = ['count']
        for x in range(24):
            increment_keys.append('h%s' % x)
        return increment_keys

    def save_to_user_action(self, rdd):
        """
        保存明细信息
        :param rdd:
        :return:
        """
        def save(data_iter):
            table = self.hbase_client.table('minik_user_action')
            save_data = {}
            for data in data_iter:
                # 构造需要保存的指标字典
                uid = str(data['uid']).zfill(8)
                # uid(8位，不足8位前面补0)-timestamp
                row_key = '%(uid)s-%(timestamp)s' % {'uid': uid, 'timestamp': data['timestamp']}
                weekday_int = DateUtil.str2date(data['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
                save_data['weekday'] = WEEKDAY[weekday_int]
                for k, v in data.items():
                    if k in ['timestamp', 'hour']:
                        continue
                    save_data[k] = v
                self.update_by_dict(table, {row_key: save_data}, [])
            self.hbase_client.close()

        rdd.foreachPartition(save)

    def count_click_metrics(self, rdd, sql_context):
        """
        按日期统计点击数
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("ui_click")

        sql = "SELECT day, user_type, action, COUNT(*) AS count FROM ui_click GROUP BY day, user_type, action"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            dashboard = DBSource.dashboard()
            column_values = []

            for row in row_iter:
                value = {}
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                value['product'] = Product.MINIK
                value['terminal'] = Terminal.MACHINE
                value['date'] = row_dict['day']
                value['user_type'] = row_dict['user_type']
                value['action'] = row_dict['action']
                value['count'] = row_dict['count']
                value['update_date'] = 'now()'
                column_values.append(value)

            self.insert_or_update_to_mysql_by_dict_list(dashboard, 'ui_click_metrics', column_values)
            dashboard.close()

        sql_df.foreachPartition(save)

    def save_to_mysql_user_action(self, rdd):
        """
        保存详细信息到mysql
        :param rdd:
        :return:
        """
        def save(column_value_iter):
            user_action = DBSource.user_action()
            # 用户行为按天分表
            column_values_group_by_day = {}
            for column_value in column_value_iter:
                # 转成字符串，数据库此值为字符串，否则当此值为int时，sql报错
                aimei_value = column_value['aimei_value']
                if isinstance(aimei_value, int) or isinstance(aimei_value, float):
                    column_value['aimei_value'] = str(aimei_value)
                day = DateUtil.timestamp2date(column_value['action_time'], '%Y%m%d')
                if day not in column_values_group_by_day:
                    column_values_group_by_day[day] = []
                day_values = column_values_group_by_day[day]
                day_values.append(column_value)
                if len(day_values) >= 1000:    # 批处理
                    table_name = 'user_action_%s' % day
                    self.insert_to_mysql_by_dict_list(user_action, table_name, day_values)
                    column_values_group_by_day[day] = []

            # 不足一批的最后处理
            for day, column_values in column_values_group_by_day.items():
                if column_values:
                    table_name = 'user_action_%s' % day
                    self.insert_to_mysql_by_dict_list(user_action, table_name, column_values)

        rdd.foreachPartition(save)

    def get_column_value(self, data):
        """
        根据行数据获取需要插入到数据库的字典
        :param data:
        :return:
        """
        action = data['action']
        timestamp = data['timestamp']
        # 流水需要的
        aimei_value = self.get_action_value(data)
        if isinstance(aimei_value, dict):
            aimei_value = json.dumps(aimei_value, ensure_ascii=False)
        if (isinstance(aimei_value, str) or isinstance(aimei_value, unicode)) and aimei_value.find("'") != -1:
            aimei_value = aimei_value.replace("'", "")  # 特殊情况处理，导致sql报错
        column_value = {
            'uid': data['uid'],
            'p_type': Product.MINIK,
            't_type': Terminal.MACHINE,
            'a_type': Action.CLICK_BUTTON,
            'session_id': data['session_id'],
            'action_time': timestamp,
            'location': data['mid'],
            'aimei_object': action,  # cf_promate_detail 定位哪个操作
            'aimei_value': aimei_value,  # 具体操作对应值，如选歌，则此处可能就是歌手id
            'update_time': 'now()'
        }
        return column_value

    def get_action_value(self, data):
        """
        获取具体行为值
        :param data:
        :return:
        """
        keys = None
        value = ''
        action = data['action']
        if action == 'cf_promate_detail':
            keys = ['period_id']
        elif action == 'cf_help_faq_detail':
            keys = ['faq_id']
        elif action == 'cc_coin_insert':
            keys = ['insert_coin_num']
        elif action in ['cc_hardware_microphone_sound', 'cc_hardware_magic_sound', 'cc_hardware_music_sound']:
            keys = ['from_value', 'to_value']
        elif action == 'cc_setting_save':
            keys = ['coin_price', 'pack_setting']
        elif action in ['cf_page_prev', 'cf_page_next']:
            keys = ['index']
        elif action == 'cf_mode_choose':
            keys = ['mode']
        elif action == 'cf_qrcode_switch':
            keys = ['name']
        elif action == 'cf_qrcode_scan':
            keys = ['error']
        elif action in ['cf_user_switch', 'cf_user_add_to']:
            keys = ['nuid']
        elif action == 'cf_wallet_package_change':
            keys = ['package_id']
        elif action == 'cf_wallet_coin_change':
            keys = ['coin']
        elif action == 'cf_wallet_ordering':
            keys = ['package_id', 'coin_num']
        elif action == 'cc_wallet_payment':
            keys = ['package_id', 'coin_num', 'pay_type']
        elif action == 'cc_wallet_pay_success':
            keys = ['package_id', 'cost_coin', 'pack_type', 'pack_type', 'pack_value']
        elif action == 'cc_mobile_pay_failed':
            keys = ['package_id', 'pack_type', 'pay_type', 'pack_value', 'needcoins', 'price', 'pay_seqid', 'orderid',
                    'cur_pay_seqid']
        elif action == 'cc_mobile_pay_timeout':
            keys = ['package_id', 'pack_type', 'pay_type', 'pack_value', 'needcoins', 'price', 'pay_seqid']
        elif action == 'cc_mobile_pay_start':
            keys = ['package_id', 'pack_type', 'pay_type', 'pack_value', 'needcoins', 'price', 'pay_seqid']
        elif action == 'cc_mobile_pay_success':
            keys = ['package_id', 'pack_type', 'pay_type', 'pack_value', 'needcoins', 'price', 'pay_seqid', 'orderid']
        elif action == 'cc_song_cost':
            keys = ['cost_type', 'cost_num', 'pay_type', 'pack_value', 'needcoins', 'price', 'pay_seqid', 'orderid']
        elif action == 'cf_singer_songs':
            keys = ['singer_id']
        elif action == 'cf_singer_search_real':
            keys = ['class_id', 'key']
        elif action == 'cf_song_search_real':
            keys = ['singer_id', 'class_id', 'key']
        elif action == 'cf_song_add':
            keys = ['song_id', 'period_id']
        elif action == 'cf_song_top':
            keys = ['song_id', 'period_id']
        elif action in ['cf_song_del', 'cf_song_fav_y', 'cf_song_fav_n', 'cf_song_channel_o', 'cf_song_channel_b',
                        'cf_song_switch', 'cf_song_listen']:
            keys = ['song_id']
        elif action == 'cf_help_faq_detail':
            keys = ['faq_id']
        elif action in ['cc_record_start', 'cc_record_end', 'cc_record_convert']:
            keys = ['song_seq', 'song_no']
        elif action in ['cc_record_start_upload', 'cc_record_upload_success', 'cc_record_upload_failed']:
            keys = ['song_no']
        elif action == 'cc_coin_cost':
            keys = ['cost_num', 'pack_type']
        elif action == 'cc_mobile_pay_add_coin':
            keys = ['orderid', 'add_coins', 'pack_type', 'pack_value', 'pay_seqid']
        elif action == 'cc_setting_recycle_coin':
            keys = ['cost_num']
        elif action == 'cc_free_ticket_get':
            keys = ['cur_song_cnt', 'cur_coupon_cnt']
        elif action == 'cc_free_ticket_get_result':
            keys = ['cur_song_cnt', 'cur_coupon_cnt', 'new_song_cnt', 'new_coupon_cnt']
        elif action == 'cc_free_ticket_use':
            keys = ['cur_song_cnt', 'cur_coupon_cnt']
        elif action == 'cc_free_ticket_use_result':
            keys = ['cur_song_cnt', 'cur_coupon_cnt', 'new_cur_coupon', 'add_coins']

        if keys:
            if len(keys) == 1:
                key = keys[0]
                if key in data:
                    value = data[key]
            else:
                value = self.get_by_keys(keys, data)
        return value

    def get_by_keys(self, keys, data):
        rtn_data = {}
        for key in keys:
            if key in data:
                value = data[key]
                rtn_data[key] = value
        return rtn_data
