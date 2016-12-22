# encoding: utf-8
"""
minik歌库服务
@author Yuriseus
@create 16-11-15 下午2:17
"""
from db.db_source import DBSource


class MinikSongService(object):
    def __init__(self):
        self.db = DBSource.minik_song()

    def get_songs(self):
        song_dict = {}
        sql = 'SELECT number,`name`,Singer1ID,Singer2ID from mk_song'
        rows = self.db.query(sql)
        for row in rows:
            number = row['number']
            song_dict[number] = row
        return song_dict

    def get_singers(self):
        singer_dict = {}
        sql = 'SELECT * from mk_singer'
        rows = self.db.query(sql)
        for row in rows:
            number = row['number']
            singer_dict[number] = row
        return singer_dict

    def get_song_singers(self):
        """
        获取歌曲对应的歌手关联信息
        @return:
        """
        song_singer_dict = {}
        sql = 'SELECT * from mk_relate WHERE app = 1'
        rows = self.db.query(sql)
        for row in rows:
            res_id = row['res_id']
            song_singer_dict[res_id] = row
        return song_singer_dict

    def get_countrys(self):
        """
        获取所有国家信息
        @return:
        """
        country_dict = {}
        sql = 'SELECT * from mk_country'
        rows = self.db.query(sql)
        for row in rows:
            number = row['number']
            country_dict[number] = row
        return country_dict

    def get_language_tags(self):
        """
        获取所有语言标签
        @return:
        """
        country_dict = {}
        sql = 'SELECT * from mk_tags WHERE channel_id=3'
        rows = self.db.query(sql)
        for row in rows:
            country_dict[row['id']] = row
        return country_dict

    def get_song_tags(self):
        """
        获取歌曲对应的标签关联信息
        @return: dict，值为标签id列表
        """
        song_tag_dict = {}
        sql = 'SELECT * from mk_relate WHERE app = 2'
        rows = self.db.query(sql)
        for row in rows:
            res_id = row['res_id']
            res_relate_id = int(row['res_relate_id'])
            if res_id not in song_tag_dict:
                song_tag_dict[res_id] = []
            tag_id_list = song_tag_dict[res_id]
            tag_id_list.append(res_relate_id)
        return song_tag_dict

    def get_banners(self):
        """
        获取推广banner信息
        @return: dict
        """
        data = {}
        sql = 'SELECT * from mk_subject'
        rows = self.db.query(sql)
        for row in rows:
            banner_id = row['id']
            data[banner_id] = row
        return data

    def query(self, sql, params_dict=None, is_fetchone=False):
        return self.db.query(sql, params_dict, is_fetchone)
