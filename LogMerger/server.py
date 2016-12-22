# encoding: utf-8
"""
主服务
@author Yuriseus
@create 2016-10-8 14:42
"""
from apscheduler.schedulers.background import BlockingScheduler
from log_merger import LogMerger
from enums import LogType
from since_util import SinceUtil


class Server(object):
    def __init__(self):
        self.cfg_list = [
            {'log_type': LogType.minik_machine_user_action, 'source_path_tpl': '/diskb/aimei_data_log/minik_machine/user_action/new/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_machine/user_action/%(year)s/%(date)s.log'},
            {'log_type': LogType.minik_server_logic, 'source_path_tpl': '/diskb/aimei_data_log/minik_server/logic_log2/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_server/logic_log/%(year)s/%(date)s.log'},
            {'log_type': LogType.minik_weixin_user_action, 'source_path_tpl': '/diskb/aimei_data_log/minik_weixin/user_action2/%(year)s/%(date)s', 'target_path_tpl': '/diskb/aimei_data_log_logstash/minik_weixin/user_action/%(year)s/%(date)s.log'}
        ]
        self.scheduler = BlockingScheduler()

    def start(self):
        self.start_merge()
        # 凌晨3点开始合并，预留时间合并前一天的
        self.scheduler.add_job(self.start_merge, 'cron', hour=3, minute=0, second=0)
        self.scheduler.start()

    def start_merge(self):
        # 清空历史记录
        since_util = SinceUtil()
        since_util.clear_history()

        merger_list = []
        for cfg in self.cfg_list:
            merger = LogMerger(cfg['log_type'], cfg['source_path_tpl'], cfg['target_path_tpl'])
            merger.start()
            merger_list.append(merger)

        for merger in merger_list:
            merger.join()


if __name__ == '__main__':
    server = Server()
    server.start()


