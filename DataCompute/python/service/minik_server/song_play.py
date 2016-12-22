# encoding: utf-8
"""
minik歌曲点播服务
@author Yuriseus
@create 16-11-07 上午10:23
"""
import json

from common.hbase_client import HbaseClient
from config.enums import WEEKDAY, Product
from db.db_source import DBSource
from service.base import BaseService
from service.minik_song_service import MinikSongService
from util.date import DateUtil


class SongPlayService(BaseService):
    def __init__(self):
        super(SongPlayService, self).__init__()
        self.hbase_client = HbaseClient()

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            str_data = tmp[3]
            str_data = str_data.replace('false', '"0"')
            str_data = str_data.replace('true', '"1"')
            d['data'] = json.loads(str_data)  # 字典
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
        # 增加登陆日期
        timestamp = int(d['timestamp'])
        if 'stime' in data:
            timestamp = int(d['stime'])
        d['timestamp'] = timestamp
        d['date'] = DateUtil.timestamp2date(timestamp, '%Y-%m-%d %H:%M:%S')
        d['hour'] = d['date'][0:13]  # 用于按小时统计
        # 增加星期
        weekday_int = DateUtil.str2date(d['date'], '%Y-%m-%d %H:%M:%S').isoweekday()
        d['weekday'] = WEEKDAY[weekday_int]
        uid = data['uid']  # str
        if uid == '':
            uid = '0'
        d['uid'] = uid
        d['mid'] = str(data['mid'])
        d['songid'] = data['songid']
        if 'stime' in data:  # 后续版本才加的，历史数据没有该项
            d['start_timestamp'] = str(data['stime'])
            d['end_timestamp'] = str(int(data['stime']) + int(data['playdur']))
            d['dur_time'] = str(data['playdur'])
            d['song_time'] = str(data['songdur'])
        d['is_finish'] = data['isfinish']
        d['is_free'] = data['isfree']
        return d

    def filter(self, d):
        """
        过滤，返回True的是需要处理的
        :param d:
        :return:
        """
        data = d['data']
        if d['interface'] == 'connector.playerHandler.playerFinishSong' and data:
            return True
        return False

    def save_to_song_play(self, data_iter):
        table = self.hbase_client.table('song_play')
        for d in data_iter:
            data = {}
            uid = d['uid'].zfill(8)
            mid = d['mid'].zfill(6)
            timestamp = DateUtil.date2timestamp(d['date'], '%Y-%m-%d %H:%M:%S')
            # product-mid(6位，不足前面补0)-uid(8位，不足8位前面补0)-timestamp
            row_key = '%(product)s-%(mid)s-%(uid)s-%(timestamp)s' % \
                      {'product': Product.MINIK, 'mid': mid, 'uid': uid, 'timestamp': timestamp}
            for k, v in d.items():
                if k not in ['hour', 'songid']:  # 统计用到的项，不入库
                    column = '%s:%s' % ('info', k)
                    data[column] = v
            table.put(row_key, data)

    def process_by_sql(self, rdd, sql_context):
        """
        使用spark sql处理
        :param rdd:
        :param sql_context:
        :return:
        """
        rdd.cache()    # 多次用到，必须cache，否则用完就销毁了，导致报错
        self.count_day_total_metrics(rdd, sql_context)
        self.count_day_total_mid_metrics(rdd, sql_context)
        self.count_song_metrics(rdd, sql_context)
        self.count_song_mid_metrics(rdd, sql_context)

    def count_day_total_metrics(self, rdd, sql_context):
        """
        统计天点播总量指标
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("song_play")

        sql = "SELECT hour, COUNT(*) AS count FROM song_play GROUP BY hour ORDER BY hour"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                count = row_dict['count']
                row_key = '%(product)s-%(type)s-%(date)s' % {'product': Product.MINIK, 'type': 'day',
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
                md['count'] += count
            table = self.hbase_client.table('song_play_total_metrics')
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def get_increment_keys(self):
        increment_keys = ['count']
        for x in range(24):
            increment_keys.append('h%s' % x)
        return increment_keys

    def count_day_total_mid_metrics(self, rdd, sql_context):
        """
        统计机器天点播总量指标
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("song_play")

        sql = "SELECT mid, hour, COUNT(*) AS count FROM song_play GROUP BY mid, hour ORDER BY mid, hour"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                count = row_dict['count']
                mid = row_dict['mid']
                # product-type-yyyymmdd-mid(6位，不足前面补0)
                row_key = '%(product)s-%(type)s-%(date)s-%(mid)s' % {'product': Product.MINIK, 'type': 'day',
                                                                     'date': date.replace('-', ''),
                                                                     'mid': str(mid).zfill(6)}
                if row_key not in metrics_dict:
                    metrics_dict[row_key] = {
                        'count': 0
                    }
                md = metrics_dict[row_key]
                md['date'] = date
                weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
                md['weekday'] = WEEKDAY[weekday_int]
                md['mid'] = row_dict['mid']
                md['h%s' % hour] = count
                md['count'] += count
            # 更新表
            table = self.hbase_client.table('song_play_total_mid_metrics')
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def count_song_metrics(self, rdd, sql_context):
        """
        按歌曲和日期统计点播数
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("song_play")

        sql = "SELECT songid, hour, COUNT(*) AS count FROM song_play GROUP BY songid, hour"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                count = row_dict['count']
                songid = row_dict['songid']
                # product-type-yyyymmdd-songid
                row_key = '%(product)s-%(type)s-%(date)s-%(songid)s' % {'product': Product.MINIK, 'type': 'day',
                                                                        'date': date.replace('-', ''), 'songid': songid}
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
            # 更新表
            table = self.hbase_client.table('song_play_metrics')
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def count_song_mid_metrics(self, rdd, sql_context):
        """
        按机器歌曲和日期统计点播数
        :param rdd:
        :param sql_context:
        :return:
        """
        df = sql_context.createDataFrame(rdd)
        self.print_schema(df)
        df.registerTempTable("song_play")

        sql = "SELECT mid, songid, hour, COUNT(*) AS count FROM song_play GROUP BY mid, songid, hour"
        sql_df = sql_context.sql(sql)

        def save(row_iter):
            metrics_dict = {}
            for row in row_iter:
                row_dict = row.asDict()
                # 构造需要保存的指标字典
                date = row_dict['hour'][0:10]
                hour = row_dict['hour'][11:13]
                mid = row_dict['mid']
                count = row_dict['count']
                songid = row_dict['songid']
                # product-type-yyyymmdd-mid(6位，不足前面补0)-songid
                row_key = '%(product)s-%(type)s-%(date)s-%(mid)s-%(songid)s' % {'product': Product.MINIK, 'type': 'day',
                                                                                'date': date.replace('-', ''),
                                                                                'mid': str(mid).zfill(6),
                                                                                'songid': songid}
                if row_key not in metrics_dict:
                    metrics_dict[row_key] = {
                        'count': 0
                    }
                md = metrics_dict[row_key]
                md['date'] = date
                weekday_int = DateUtil.str2date(date, '%Y-%m-%d').isoweekday()
                md['weekday'] = WEEKDAY[weekday_int]
                md['mid'] = mid
                md['h%s' % hour] = count
                md['count'] += count
            # 更新表
            table = self.hbase_client.table('song_play_mid_metrics')
            increment_keys = self.get_increment_keys()
            self.update_by_dict(table, metrics_dict, increment_keys)
            self.hbase_client.close()

        sql_df.foreachPartition(save)

    def save_to_mysql(self, rdd):
        """
        保存到mysql
        :param rdd:
        :return:
        """
        song_service = MinikSongService()
        songs = song_service.get_songs()

        def save(d_iter):
            dashboard = DBSource.dashboard()
            column_values = []
            for d in d_iter:
                value = {}
                value['mid'] = int(d['mid'])
                value['province'] = ''
                value['city'] = ''
                value['uid'] = int(d['uid'])
                song_id = d['songid']
                value['song_id'] = song_id
                singer_id = ''
                if song_id in songs:
                    song = songs[song_id]
                    singer1_id = song['Singer1ID']
                    singer_id = str(singer1_id)
                    singer2_id = song['Singer2ID']
                    if singer2_id != 0:
                        singer_id += '|' + str(singer2_id)
                value['singer_id'] = singer_id
                value['is_free'] = int(d['is_free'])
                value['is_finish'] = int(d['is_finish'])
                if 'start_timestamp' in d:    # 新接口才加了时间
                    value['start_date'] = DateUtil.timestamp2date(int(d['start_timestamp']), '%Y-%m-%d %H:%M:%S')
                    value['dur_time'] = int(d['dur_time'])
                    value['song_time'] = int(d['song_time'])
                else:
                    value['start_date'] = ''
                    value['dur_time'] = 0
                    value['song_time'] = 0
                value['update_date'] = 'now()'

                column_values.append(value)
            self.insert_to_mysql_by_dict_list(dashboard, 'minik_song_play', column_values)
            dashboard.close()
        rdd.foreachPartition(save)
