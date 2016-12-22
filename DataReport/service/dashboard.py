# encoding: utf-8
"""
dashboard数据库查询服务
@author Yuriseus
@create 2016-12-8 14:49
"""
from service.base import BaseService
from config.enums import *
from util.date import DateUtil


class DashboardService(BaseService):

    def __init__(self):
        super().__init__()

    def get_user_count_by_date(self, date, product=Product.MINIK, terminal=Terminal.MACHINE):
        """
        按天获取用户数量
        :param product:
        :param terminal:
        :param date: yyyy-mm-dd
        :return dict:
        """
        count_data = {
            'date': date,
            'total': 0,
            'add': 0,
            'lost': 0,
            'dau': 0
        }
        sql1 = "SELECT type, SUM(count) AS count FROM user_count_metrics WHERE product = %s AND terminal = %s " \
               "AND date = '%s' GROUP BY type" % (product, terminal, date)
        sql2 = "SELECT type, SUM(count) AS count  FROM user_active_metrics WHERE product = %s AND terminal = %s " \
               "AND date = '%s' GROUP BY type" % (product, terminal, date)
        count_rows = self.db_dashboard.query(sql1)
        active_rows = self.db_dashboard.query(sql2)
        for row in count_rows:
            c_type = int(row['type'])
            count = int(row['count'])
            if c_type == UserCountType.TOTAL:
                count_data['total'] = count
            elif c_type == UserCountType.ADD:
                count_data['add'] = count
            elif c_type == UserCountType.LOST:
                count_data['lost'] = count
        # 活跃
        for row in active_rows:
            a_type = int(row['type'])
            count = int(row['count'])
            if a_type == UserActiveType.DAU:
                count_data['dau'] = count
        return count_data

    def get_game_session_by_date_range(self, start_date, end_date):
        """
        获取所有游戏局
        :param start_date: yyyy-mm-dd
        :param end_date: yyyy-mm-dd
        :return:
        """
        sql = "SELECT * FROM minik_game_session WHERE start_date >= '%s 00:00:00' AND start_date <= '%s 23:59:59'" % (start_date, end_date)
        return self.db_dashboard.query(sql)

    def get_game_session_metrics_by_date_range(self, start_date, end_date):
        metrics_data = []
        date_metrics = {}
        sessions = self.get_game_session_by_date_range(start_date, end_date)
        for s in sessions:
            date = str(s['start_date'])[0:10]
            dur_time = s['dur_time']
            uid = s['uid']
            mode_type = s['mode_type']
            if date not in date_metrics:
                date_metrics[date] = {
                    'date': date,
                    'total': 0,
                    'free': 0,
                    'cost': 0,
                    'anonymous': 0,
                    'login': 0,
                    'user_count': 0,
                }
            metrics = date_metrics[date]
            metrics['total'] += 1
            if mode_type == 0:
                metrics['free'] += 1
            else:
                metrics['cost'] += 1
            if uid:
                metrics['login'] += len(uid.split(','))
            else:
                metrics['anonymous'] += 1

        # 按日期倒序
        tuple_list = sorted(date_metrics.items(), key=lambda d: d[0], reverse=True)
        for tu in tuple_list:
            metrics_data.append(tu[1])
        return metrics_data

    def get_song_play_count_by_date(self, date):
        """
        获取歌曲点播量，按天
        :param date:
        :return:
        """
        count_data = {
            'date': date,
            'total': 0,
            'free': 0,
            'cost': 0,
            'anonymous': 0,
            'login': 0,
        }
        sql = "SELECT is_free, uid, count(*) AS count FROM minik_song_play WHERE start_date >= '%s 00:00:00' AND " \
              "start_date <= '%s 23:59:59' GROUP BY uid != '', is_free" % (date, date)
        rows = self.db_dashboard.query(sql)
        for row in rows:
            is_free = row['is_free']
            uid = int(row['uid'])
            count = row['count']
            count_data['total'] += count
            if is_free == 0:
                count_data['cost'] += count
            else:
                count_data['free'] += count
            if uid == 0:
                count_data['anonymous'] += count
            else:
                count_data['login'] += count
        return count_data

    def get_top_singer_play(self, date, top=10):
        """
        获取歌手点播排行
        :param date:
        :param top:
        :return:
        """
        top_singer = {'date': date, 'singers': []}
        singers = self.get_singers()
        sql = "SELECT singer_id, count(*) AS count FROM minik_song_play WHERE start_date >= '%s 00:00:00' AND " \
              "start_date <= '%s 23:59:59' GROUP BY singer_id ORDER BY count DESC LIMIT %s" % (date, date, top)
        rows = self.db_dashboard.query(sql)
        for row in rows:
            singer_id_str = row['singer_id']
            count = row['count']
            singer_names = []
            singer_ids = singer_id_str.split('|')    # 可能有多个
            for singer_id in singer_ids:
                if singer_id and int(singer_id) in singers:
                    singer_names.append(singers[int(singer_id)]['name'])
            top_singer['singers'].append({'singer_id': singer_id_str, 'singer_name': '、'.join(singer_names), 'count': count})
        return top_singer

    def get_top_song_play(self, date, top=10):
        """
        获取歌曲点播排行
        :param date:
        :param top:
        :return:
        """
        top_song = {'date': date, 'songs': []}
        songs = self.get_songs()
        sql = "SELECT song_id, count(*) AS count FROM minik_song_play WHERE start_date >= '%s 00:00:00' AND " \
              "start_date <= '%s 23:59:59' GROUP BY song_id ORDER BY count DESC LIMIT %s" % (date, date, top)
        rows = self.db_dashboard.query(sql)
        for row in rows:
            song_id = row['song_id']
            count = row['count']
            song_name = ''
            if song_id in songs:
                song_name = songs[song_id]['name']
            top_song['songs'].append({'song_id': song_id, 'song_name': song_name, 'count': count})
        return top_song

    def get_singers(self, product=Product.MINIK):
        singers = {}
        sql = "SELECT * FROM  base_singer WHERE product=%s" % product
        rows = self.db_dashboard.query(sql)
        for row in rows:
            singer_id = row['number']
            singers[singer_id] = row
        return singers

    def get_songs(self, product=Product.MINIK):
        songs = {}
        sql = "SELECT * FROM  base_song WHERE product=%s" % product
        rows = self.db_dashboard.query(sql)
        for row in rows:
            song_id = row['number']
            songs[song_id] = row
        return songs


if __name__ == '__main__':
    s = DashboardService()
    # count = s.get_user_count_by_date('2016-12-07')
    # count = s.get_song_play_count_by_date('2016-12-07')
    count = s.get_top_singer_play('2016-12-08')
    print(count)


