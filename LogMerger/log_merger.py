# encoding: utf-8
"""
日志合并，按收集日志的日期和日志类型合并日志收集工具上传的原始日志，供logstash传送至kafka
@author Yuriseus
@create 2016-10-08 14:16
"""

import datetime
import os
import time
from importlib import reload
from threading import Thread

import setting
from enums import LogType
from file_util import FileUtil
from log import logger
from since_util import SinceUtil


class LogMerger(Thread):

    def __init__(self, log_type, source_path_tpl, target_path_tpl):
        # self.source_path_tpl = '/diskb/aimei_data_log/minik_machine/user_action/unpack_2/2016/%s'
        # self.target_path_tpl = '/diskb/aimei_data_log_logstash/minik_machine/user_action/2016/%s.log'
        super().__init__()
        self.log_type = log_type
        self.source_path_tpl = source_path_tpl
        self.target_path_tpl = target_path_tpl
        self.source_path = None
        self.target_path = None
        self.need_merge_file_list = []
        self.completed_logs = {}    # 已合并过的日志
        self.uncompleted_logs = {}    # 未合并完成的日志
        self.merge_date = None
        self.since_util = SinceUtil()
        self.target_file = None

    def reset(self):
        self.source_path = None
        self.target_path = None
        self.need_merge_file_list = []
        self.since_util = SinceUtil()
        if self.target_file:
            self.target_file.close()
            self.target_file = None
        self.completed_logs = self.since_util.get_records_by_status(1)    # 已处理完成的
        self.uncompleted_logs = self.since_util.get_records_by_status(0)    # 未处理完成的

    def run(self):
        self.merge_by_date()

    def merge_by_date(self, date=None):
        """
        开始合并
        :param date:
        :return:
        """
        self.reset()
        if not date:
            date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.merge_date = date
        logger.info('start merge %s date %s' % (self.log_type, date), self.log_type)
        year = date[0:4]
        self.source_path = self.source_path_tpl % {'year': year, 'date': date}
        self.target_path = self.target_path_tpl % {'year': year, 'date': date}
        self.target_file = open(self.target_path, 'a+')
        while True:
            while self.need_merge_file_list:
                reload(setting)
                if not setting.IS_RUNNING:
                    break
                file_path = self.need_merge_file_list.pop()
                self.write_to_file(file_path)
            logger.info('='*20 + ' check new log %s ' % self.log_type + '*'*20)
            self.check_new_log()
            time.sleep(5)
            now = datetime.datetime.now()
            reload(setting)
            if now.hour == 1 or not setting.IS_RUNNING:    # 凌晨1点结束前一天的新文件检测
                break

    def check_new_log(self):
        """
        检测新文件加入待合并列表
        :return:
        """
        is_has_new = False
        tmp_file_list = FileUtil.get_files(self.source_path, ['.COMPLETE', '.gz'], self.is_filter_file)
        for file_path in tmp_file_list:
            if file_path not in self.completed_logs:
                # 判断时间
                create_time = os.path.getctime(file_path)
                if time.time() - create_time >= 30:
                    self.need_merge_file_list.append(file_path)
                    self.completed_logs[file_path] = 0
                    is_has_new = True
        if not is_has_new:
            logger.info('='*20 + ' has not new log %s ' % self.log_type + '*'*20)

    def is_filter_file(self, file_path):
        """
        是否过滤掉该文件
        :param file_path:
        :return:
        """
        file_name = file_path.split('/')[-1]
        if file_name.startswith('.') or file_name.find('.gz') != -1:
            return True
        if self.log_type == LogType.minik_weixin_user_action and file_name == 'user_action.log':
            # 未切割的日志不能合并，否则会重复合并
            return True
        return False

    def write_to_file(self, file_path, to_file_path=None):
        """
        日志内容除过滤掉错误的和空行外，其他都原样写入
        :param file_path:
        :param to_file_path:
        :return:
        """
        logger.info("merge: " + file_path, self.log_type)
        self.since_util.add_2_db(file_path, self.log_type)
        if to_file_path:
            wf = open(to_file_path, 'a+')
        else:
            wf = self.target_file
        line_no = 1
        start_time = 0
        with open(file_path, 'r') as rf:
            last_line = ''
            while True:
                merged_line = -1
                if file_path in self.uncompleted_logs:
                    merged_line = self.uncompleted_logs[file_path]
                try:
                    if line_no < merged_line:    # 已经合并过
                        line_no += 1
                        line = rf.readline()
                        continue
                    line = rf.readline()
                    if not line:    # 文件读取结束，合并完成
                        break
                    if line and line != '\n':    # 去掉空行
                        wf.write(line)
                    line_no += 1
                    last_line = line
                    self.uncompleted_logs[file_path] = line_no

                    # 每1秒刷一次数据库，提高处理效率。因为一个文件合并时间是在大多数在1秒之内。如果程序意外挂掉，数据不会丢失，而是会重复合并1s内能处理的数据
                    if time.time() - start_time >= 1:
                        self.since_util.update_2_db(file_path, line_no)
                        start_time = time.time()

                except UnicodeDecodeError as e:
                    logger.warning('UnicodeDecodeError:%s, filepath:%s, line_no:%s' % (str(e), file_path, line_no+1), self.log_type)
                    rf.seek(rf.tell() + 1)
                    line_no += 1
                    if last_line == '':
                        break
                except Exception as e:
                    logger.warning('error:%s, filepath:%s, line_no:%s' % (str(e), file_path, line_no+1), self.log_type)
                    rf.seek(rf.tell() + 1)

        # 处理完成，更新状态，刷一次处理行数
        self.since_util.update_2_complete(file_path)
        self.since_util.update_2_db(file_path, line_no)


if __name__ == '__main__':
    merger = LogMerger('test', 1, 1)
    # merger.merge_by_date()
    f = '/diskb/aimei_data_log/minik_machine/user_action/new/2016/2016-10-27/.3040_data_collect.log.2016-10-27-14.C1maqP'
    print(merger.is_filter_file(f))

