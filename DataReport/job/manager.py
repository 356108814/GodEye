# encoding: utf-8
"""
任务管理器
@author Yuriseus
@create 2016-9-23 14:39
"""
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import config.settings as settings
from job.daily_report import DailyReportJob
from util.date import DateUtil


class JobManager(object):

    def __init__(self):
        self.scheduler = BlockingScheduler()

    def start(self):
        self.scheduler.add_job(self.send_daily_report, 'cron', hour=9, minute=10, second=0)
        self.scheduler.start()

    def send_daily_report(self):
        """
        发送minik日报
        :return:
        """
        now = datetime.datetime.now()
        yesterday = DateUtil.get_interval_day_date(now, -1)
        daily_report = DailyReportJob()
        daily_report.send(yesterday, settings.DAILY_REPORT_TO_MAILS)

if __name__ == '__main__':
    job = JobManager()
    job.start()


