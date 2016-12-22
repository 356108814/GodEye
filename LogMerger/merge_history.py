# encoding: utf-8
"""
合并历史数据
@author Yuriseus
@create 2016-11-2 10:26
"""
import datetime

from date_util import DateUtil
from enums import LogType
from file_util import FileUtil
from log import logger


class HistoryMerger(object):

    def __init__(self, log_type, source_path_tpl, target_path_tpl):
        self.log_type = log_type
        self.source_path_tpl = source_path_tpl
        self.target_path_tpl = target_path_tpl
        self.source_path = None
        self.target_path = None
        self.need_merge_file_list = []
        self.completed_logs = {}    # 已合并过的日志
        self.uncompleted_logs = {}    # 未合并完成的日志
        self.merge_date = None
        self.target_file = None

    def reset(self):
        self.source_path = None
        self.target_path = None
        self.need_merge_file_list = []
        if self.target_file:
            self.target_file.close()
            self.target_file = None

    def merge_by_date(self, date=None):
        self.reset()
        if not date:
            date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.merge_date = date
        logger.info('start merge %s date %s' % (self.log_type, date), self.log_type)
        year = date[0:4]
        self.source_path = self.source_path_tpl % {'year': year, 'date': date}
        self.target_path = self.target_path_tpl % {'year': year, 'date': date}
        # self.source_path = 'J:\\ls'
        # self.target_path = 'J:\\s\\t.log'

        self.target_file = open(self.target_path, 'a+')
        self.need_merge_file_list = FileUtil.get_files(self.source_path, ['.COMPLETE', '.gz'], self.is_filter_file)
        while self.need_merge_file_list:
            file_path = self.need_merge_file_list.pop()
            self.write_to_file(file_path)

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
        if to_file_path:
            wf = open(to_file_path, 'a+')
        else:
            wf = self.target_file
        line_no = 1
        with open(file_path, 'r+', encoding='utf-8') as rf:
            while True:
                last_line = ''
                try:
                    line = rf.readline()
                    if not line:    # 文件读取结束，合并完成
                        break
                    if line and line != '\n':    # 去掉空行
                        wf.write(line)
                    line_no += 1
                    last_line = line
                except UnicodeDecodeError as e:
                    logger.warning('UnicodeDecodeError:%s, filepath:%s, line_no:%s' % (str(e), file_path, line_no+1), self.log_type)
                    line_no += 1
                    if last_line == '':
                        break
                except Exception as e:
                    logger.warning('error:%s, filepath:%s, line_no:%s' % (str(e), file_path, line_no+1), self.log_type)
                    raise


if __name__ == '__main__':
    cfg_list = [
        #{'log_type': LogType.minik_machine_user_action, 'source_path_tpl': '/diskb/aimei_data_log/minik_machine/user_action/new/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_machine/user_action/%(year)s/%(date)s.log'},
        {'log_type': LogType.minik_server_logic, 'source_path_tpl': '/diskb/aimei_data_log/minik_server/logic_log2/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_server/logic_log/%(year)s/%(date)s.log'},
        #{'log_type': LogType.minik_weixin_user_action, 'source_path_tpl': '/diskb/aimei_data_log/minik_weixin/user_action2/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_weixin/user_action/%(year)s/%(date)s.log'}
    ]
    start_date = '2016-10-09'
    end_date = '2016-10-09'
    date_list = DateUtil.get_range_date_list(start_date, end_date)
    for date in date_list:
        for cfg in cfg_list:
            merger = HistoryMerger(cfg['log_type'], cfg['source_path_tpl'], cfg['target_path_tpl'])
            merger.merge_by_date(date)
