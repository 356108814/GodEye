# encoding: utf-8
"""
用户行为
@author Yuriseus
@create 2016-12-07 15:14
"""
from config.enums import *
from db.db_source import DBSource
from service.base import BaseService
from util.date import DateUtil
from util.socket_util import SocketUtil


class UserActionService(BaseService):
    def __init__(self):
        super(UserActionService, self).__init__()

    def split(self, line):
        # 格式：[I 160906 16:57:56 record_handler:189] {"action_time": 1473152276, "songid": "6025300", "uid": 3070334, "ip": "172.16.30.3", "action": 0, "aimei_object": "RecordPlay"}
        # 去除日志前缀
        index = line.find(']')
        line = (line[index + 1:]).strip()
        data = self.parser.get_dict(line)
        return data

    def process_data(self, d):
        # 增加
        date = DateUtil.timestamp2date(d['action_time'], fmt='%Y-%m-%d %H:%M:%S')
        d['action'] = d['aimei_object']
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
        if 'uid' not in d:
            return False
        return True

    def get_column_value(self, data):
        """
        根据行数据获取需要插入到数据库的字典
        :param data: 清洗后的数据
        :return:
        """
        # 流水需要的
        action_time = data['action_time']
        aimei_object = data['aimei_object']
        column_value = {
            'uid': data['uid'],
            'p_type': Product.MINIK,
            't_type': Terminal.WEIXIN,
            'a_type': data['action'],
            'session_id': 0,    # 移动端行为没有局的概念
            'action_time': action_time,
            'location': SocketUtil.ip2int(data['ip']),
            'aimei_object': aimei_object,
            'aimei_value': '',
            'update_time': 'now()'
        }

        if aimei_object == 'album':
            column_value['aimei_value'] = data['albumid']
        elif aimei_object == 'RecordPlay':
            column_value['aimei_value'] = data['songid']
        return column_value

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
                value['terminal'] = Terminal.WEIXIN
                value['date'] = row_dict['day']
                value['user_type'] = row_dict['user_type']
                value['action'] = row_dict['action']
                value['count'] = row_dict['count']
                value['update_date'] = 'now()'
                column_values.append(value)

            self.insert_or_update_to_mysql_by_dict_list(dashboard, 'ui_click_metrics', column_values)
            dashboard.close()

        sql_df.foreachPartition(save)

