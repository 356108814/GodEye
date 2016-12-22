# encoding: utf-8
"""
日常报表
@author Yuriseus
@create 2016-12-8 14:46
"""
from jinja2 import Template

import datetime
import os
import config.settings as settings
from service.email_sender import EmailSender
from service.data import DataService
from service.dashboard import DashboardService
from util.date import DateUtil


class DailyReportJob(object):
    def __init__(self):
        self.data = DataService()
        self.dashboard = DashboardService()

    def send(self, date, to_mails):
        """
        发送日报，日报内容包括最近一周的数据，为了方便对比
        :param date:
        :param to_mails: 收件人邮箱
        :return:
        """
        start_date = DateUtil.get_interval_day_date(date, -2)
        # 营收
        incomes = self.data.get_income_by_date_range(start_date, date)
        incomes.sort(key=lambda d: d['date'], reverse=True)
        # 用户数量
        user_counts = self.dashboard.get_by_date_range(start_date, date, self.dashboard.get_user_count_by_date)
        user_counts.sort(key=lambda d: d['date'], reverse=True)
        # 局数
        game_sessions = self.dashboard.get_game_session_metrics_by_date_range(start_date, date)
        # 点播量
        play_counts = self.dashboard.get_by_date_range(start_date, date, self.dashboard.get_song_play_count_by_date)
        play_counts.sort(key=lambda d: d['date'], reverse=True)
        # 歌手点播排行
        top_singers = self.dashboard.get_by_date_range(start_date, date, self.dashboard.get_top_singer_play)
        top_singers.sort(key=lambda d: d['date'], reverse=True)
        # 歌曲点播排行
        top_songs = self.dashboard.get_by_date_range(start_date, date, self.dashboard.get_top_song_play)
        top_songs.sort(key=lambda d: d['date'], reverse=True)

        # 概况数据
        revenue = {'total': incomes[0]['total'], 'increment': incomes[0]['total'] - incomes[1]['total']}
        user_count = {'dau': user_counts[0]['dau'], 'dau_increment': user_counts[0]['dau'] - user_counts[1]['dau'],
                      'growth': user_counts[0]['add'] - user_counts[0]['lost'],
                      'growth_increment': (user_counts[0]['add'] - user_counts[0]['lost']) - (user_counts[1]['add'] - user_counts[1]['lost'])}
        game_session = {'total': game_sessions[0]['total'], 'increment': game_sessions[0]['total'] - game_sessions[1]['total']}
        play_count = {'total': play_counts[0]['total'], 'increment': play_counts[0]['total'] - play_counts[1]['total']}

        # 歌手排行分析，先比较排行内容，再比较名次
        new_in, old_out, rank_changed = self.compare_rank('singer_id', top_singers[0]['singers'], top_singers[1]['singers'])
        singer = {'new_in': new_in, 'old_out': old_out, 'rank_changed': rank_changed}
        new_in, old_out, rank_changed = self.compare_rank('song_id', top_songs[0]['songs'], top_songs[1]['songs'])
        song = {'new_in': new_in, 'old_out': old_out, 'rank_changed': rank_changed}
        overview = {
            'revenue': revenue,
            'user_count': user_count,
            'game_session': game_session,
            'play_count': play_count,
            'singer': singer,
            'song': song
        }

        title = u'%sminik运营概况' % date
        sender = EmailSender()
        param = {'title': title, 'incomes': incomes, 'user_counts': user_counts, 'game_sessions': game_sessions,
                 'play_counts': play_counts, 'top_singers': top_singers, 'top_songs': top_songs, 'overview': overview}
        tpl_path = os.path.join(settings.BASE_DIR, 'template/daily_report.html')
        with open(tpl_path, 'r', encoding='utf-8') as f:
            tpl = f.read()
        template = Template(tpl)
        text = template.render(param)    # 注意模板默认编码为Unicode，因此param中的中文必须也为Unicode
        if settings.DEBUG:
            write_path = os.path.join(settings.BASE_DIR, 'template/daily_report_view.html')
            with open(write_path, 'w', encoding='utf-8') as f:
                f.write(text)
        else:
            sender.send_mail(to_mails, title, text)

    def compare_rank(self, unique_key, new_list, old_list):
        """
        排名比较
        :param unique_key: 用来比较的唯一键
        :param new_list: 新排行
        :param old_list : 旧排行
        :return: new_in 新进榜单, old_out 跌出榜单, rank_changed 排名变化的
        """
        new_dict = {}
        old_dict = {}
        for rank, new in enumerate(new_list):
            new_dict[new[unique_key]] = {'rank': rank + 1, 'data': new}
        for rank, old in enumerate(old_list):
            old_dict[old[unique_key]] = {'rank': rank + 1, 'data': old}

        new_keys = [new[unique_key] for new in new_list]
        old_keys = [old[unique_key] for old in old_list]
        new_in_keys = list(set(new_keys).difference(set(old_keys)))    # new_keys中有而old_keys中没有的，即新进榜单的
        old_out_keys = list(set(old_keys).difference(set(new_keys)))    # old_keys中有而new_keys中没有的，即跌出榜单的
        # 排名变化的
        rank_changed = []
        intersection = list(set(new_keys).intersection(set(old_keys)))
        for key in intersection:
            new = new_dict[key]
            old = old_dict[key]
            if new['rank'] != old['rank']:
                rank_changed.append({'new': new, 'old': old})
        new_in = []
        old_out = []
        for key in new_in_keys:
            new_in.append(new_dict[key])
        for key in old_out_keys:
            old_out.append(old_dict[key])
        return new_in, old_out, rank_changed

    # def compare_singer_rank(self, start_date, end_date):
    #     top_singers = self.dashboard.get_by_date_range(start_date, end_date, self.dashboard.get_top_singer_play)
    #     top_singers.sort(key=lambda d: d['date'], reverse=True)
    #     # return self.compare_rank('singer_id', top_singers[0]['singers'], top_singers[1]['singers'])

    # def compare_song_rank(self, start_date, end_date):
    #     top_songs = self.dashboard.get_by_date_range(start_date, end_date, self.dashboard.get_top_song_play)
    #     top_songs.sort(key=lambda d: d['date'], reverse=True)
    #     return self.compare_rank('song_id', top_songs[0]['songs'], top_songs[1]['songs'])


if __name__ == '__main__':
    now = datetime.datetime.now()
    yesterday = DateUtil.get_interval_day_date(now, -1)
    job = DailyReportJob()
    job.send(yesterday, None)



