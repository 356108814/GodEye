# encoding: utf-8
"""
机器服务器逻辑日志ETL。
作用是读取然后按照日期和接口合并到一天，用于数据验证。因为目前日志里面的日期是收集日期。
@author Yuriseus
@create 16-11-17 下午4:34
"""
import json
import os

from service.minik_server.active_user import ActiveUserService
from service.minik_server.consume import ConsumeService
from service.minik_server.game_session import GameSessionService
from service.minik_server.show_user_info import ShowUserInfoService
from service.minik_server.song_play import SongPlayService
from util.date import DateUtil


class MinikServerInterfaceETL(object):
    def __init__(self):
        super(MinikServerInterfaceETL, self).__init__()
        self.files = {}

    def process_start(self, date):
        """
        开始处理
        :param date: yyyy-mm-dd
        :return:
        """
        year = date[0:4]
        source_path = '/data/disk1/logdata/minik_server/logic/%s/%s.log' % (year, date)
        self.process_file(source_path)

    def process_file(self, file_path):
        with open(file_path) as f:
            for line in f:
                d = self.split(line)
                if self.filter(d):
                    self.process_after_clean(d)
        self.process_end()

    def split(self, line):
        d = {}
        line = line.replace('\n', '')
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['mid'] = tmp[1]
        d['interface'] = tmp[2]
        try:
            # 因为有异常情况导致解析的日志中的格式是错误的
            d['data'] = json.loads(tmp[3])    # 字典
        except Exception:
            d['data'] = None
        d['exec_version'] = tmp[4]    # 程序版本
        d['ui_version'] = tmp[5]    # UI版本
        return d

    def filter(self, d):
        data = d['data']
        if data:
            return True
        return False

    def process_after_clean(self, d):
        interface = d['interface']
        data = self.get_clean_data(d, interface)

        if data:
            # 数据必须包含时间戳
            timestamp = int(data['timestamp'])
            if timestamp >= 1451577600:    # 2016-01-01 00:00:00
                date = DateUtil.timestamp2date(timestamp, '%Y-%m-%d')
                year = date[0:4]
                data = str(data).replace("u'", '"')    # 标准json是双引号的
                data = data.replace("'", '"')
                # 目录不存在需要创建
                base_dir = '/data/disk1/clean_data/minik_server/logic/%(year)s/%(date)s/' % {'year': year, 'date': date}
                if not os.path.exists(base_dir):
                    os.makedirs(base_dir)

                # 每个接口一个文件
                interface_file_path = base_dir + '%(interface)s.log' % {'interface': interface}
                if interface_file_path not in self.files:
                    target_file = open(interface_file_path, 'a+')
                    self.files[interface_file_path] = target_file

                target_file = self.files[interface_file_path]
                target_file.write('%s\n' % data)

    def get_clean_data(self, d, interface):
        """
        获取清洗后的数据。
        :param d:
        :param interface:
        :return: dict 必须包含时间戳timestamp
        """
        data = None

        if interface == 'connector.entryHandler.macPlayingModeGameOverV2':
            service = GameSessionService()
            if service.filter(d):
                data = service.process_data(d)
        elif interface == 'connector.playerHandler.playerFinishSong':
            service = SongPlayService()
            if service.filter(d):
                data = service.process_data(d)
        elif interface == 'onWebxinLogin':
            service = ActiveUserService()
            if service.filter(d):
                data = service.process_data(d)
        elif interface == 'onShowUserInfo':
            service = ShowUserInfoService()
            if service.filter(d):
                data = service.process_data(d)
        elif interface == 'connector.entryHandler.accountRecordInfo':
            service = ConsumeService()
            if service.filter(d):
                data = service.process_data(d)
        else:
            if self.filter(d):
                data = d['data']
                data['timestamp'] = d['timestamp']
        return data

    def process_end(self):
        """
        处理结束，关闭所有打开的文件
        :return:
        """
        for file_path, file_handler in self.files.items():
            file_handler.close()




