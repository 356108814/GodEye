# encoding: utf-8
"""
用户选歌统计服务
@author Yuriseus
@create 16-11-8 上午11:17
"""

import json

from common.hbase_client import HbaseClient
from config.enums import WEEKDAY, Product
from service.base import BaseService
from util.date import DateUtil


class SongSelectService(BaseService):
    def __init__(self):
        super(SongSelectService, self).__init__()
        self.hbase_client = HbaseClient()

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            d['data'] = json.loads(tmp[3])  # 字典
        except Exception as e:
            d['data'] = None
        d['exec_version'] = tmp[4]  # 程序版本
        d['ui_version'] = tmp[5]  # UI版本
        return d

    def process_data(self, d):
        """
        处理data数据，分解data字典，去除空数据
        :param d:
        :return:
        """
        data = d['data']
        del d['data']
        for k, v in data.items():
            if v == '' or (isinstance(v, list) and not v):
                continue
            d[k] = v
        timestamp = int(d['timestamp'])
        d['date'] = DateUtil.timestamp2date(timestamp, '%Y-%m-%d %H:%M:%S')
        d['hour'] = d['date'][0:13]  # 用于按小时统计
        # 增加星期
        weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        d['song_id'] = data['songid']
        d['singer_id'] = data['singer_no']
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'connector.playerHandler.selectSong' and data:
            return True
        return False

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        self.count_select_metrics(rdd, sql_context)

    def get_increment_keys(self):
        increment_keys = ['count']
        for x in range(24):
            increment_keys.append('h%s' % x)
        return increment_keys

    def count_select_metrics(self, rdd, sql_context):
        """
        按歌曲和日期统计点播数
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("song_select")

        sql = "SELECT song_id, singer_id, hour, COUNT(*) AS count FROM song_select GROUP BY song_id, singer_id, hour"
        sql_df = sql_context.sql(sql)

        sql_df.foreachPartition(self.save_by_row_iter)

    def save_by_row_iter(self, row_iter):
        metrics_dict = {}
        for row in row_iter:
            row_dict = row.asDict()
            # 构造需要保存的指标字典
            date = row_dict['hour'][0:10]
            hour = row_dict['hour'][11:13]
            count = row_dict['count']
            song_id = row_dict['song_id']
            singer_id = row_dict['singer_id']
            # product-type-yyyymmdd-songid
            row_key = '%(product)s-%(type)s-%(date)s-%(song_id)s' % {'product': Product.MINIK, 'type': 'day',
                                                                     'date': date.replace('-', ''), 'song_id': song_id}
            if row_key not in metrics_dict:
                metrics_dict[row_key] = {
                    'count': 0
                }
            md = metrics_dict[row_key]
            md['date'] = date
            weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
            md['weekday'] = WEEKDAY[weekday_int]
            md['h%s' % hour] = count
            md['count'] += count
            md['song_id'] = song_id
            md['singer_id'] = singer_id

        # 更新表
        table = self.hbase_client.table('song_select_metrics')
        increment_keys = self.get_increment_keys()
        self.update_by_dict(table, metrics_dict, increment_keys)
        self.hbase_client.close()

