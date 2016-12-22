# encoding: utf-8
"""
kshow歌库服务
@author Yuriseus
@create 16-11-24 上午11:12
"""
from db.db_source import DBSource


class KshowSongService(object):
    def __init__(self):
        self.db = DBSource.kshow_song()

    def get_song_singer(self):
        """
        获取歌曲属于的歌手
        :return: dict 键为ks_song_id或ks_song_num。值为ks_singer_num
        """
        data = {}
        singers = self.get_singers()
        rows = self.db.query("select * from ks_song_singer")
        for row in rows:
            # ks_song_id = row['ks_song_id']
            ks_song_num = row['ks_song_num']
            ks_singer_num = row['ks_singer_num']
            singer = singers[ks_singer_num]
            # data[ks_song_id] = singer
            if ks_song_num not in data:    # 一个歌曲有个多歌手
                data[ks_song_num] = []
            data[ks_song_num].append(singer)
        return data

    def get_singers(self):
        """
        获取所有歌手信息
        :return: dict
        """
        data = {}
        rows = self.db.query("select * from ks_singer")
        for row in rows:
            number = row['number']
            data[number] = row
        return data

    def query(self, sql, params_dict=None, is_fetchone=False):
        return self.db.query(sql, params_dict, is_fetchone)







