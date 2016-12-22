# encoding: utf-8
"""
机器行为日志ETL
@author Yuriseus
@create 16-11-17 下午4:33
"""
import os

from service.minik_machine.user_action import UserActionService
from util.date import DateUtil

service = UserActionService()


class MinikMachineETL(object):
    def __init__(self):
        super(MinikMachineETL, self).__init__()
        self.files = {}

    def process_start(self, date):
        """
        开始处理
        :param date: yyyy-mm-dd
        :return:
        """
        year = date[0:4]
        source_path = '/data/disk1/logdata/minik_machine/user_action/%s/%s.log' % (year, date)
        self.process_file(source_path)

    def process_file(self, file_path):
        with open(file_path) as f:
            for line in f:
                d = service.split(line)
                if service.filter(d):
                    d = service.process_data(d)
                    self.process_after_clean(d)
        self.process_end()

    def process_after_clean(self, data):
        if data:
            # 数据必须包含时间戳
            timestamp = int(data['timestamp'])
            if timestamp >= 1451577600:    # 2016-01-01 00:00:00
                date = DateUtil.timestamp2date(timestamp, '%Y-%m-%d')
                year = date[0:4]
                data = str(data).replace("u'", '"')    # 标准json是双引号的
                data = data.replace("'", '"')
                # 目录不存在需要创建
                base_dir = '/data/disk1/clean_data/minik_machine/user_action/%(year)s/' % {'year': year}
                if not os.path.exists(base_dir):
                    os.makedirs(base_dir)

                # 每天一个文件
                target_file_path = base_dir + '%(date)s.log' % {'date': date}
                if target_file_path not in self.files:
                    target_file = open(target_file_path, 'a+')
                    self.files[target_file_path] = target_file

                target_file = self.files[target_file_path]
                target_file.write('%s\n' % data)

    def process_end(self):
        """
        处理结束，关闭所有打开的文件
        :return:
        """
        for file_path, file_handler in self.files.items():
            file_handler.close()
