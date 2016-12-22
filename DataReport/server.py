# encoding: utf-8
"""
服务器启动入口
@author Yuriseus
@create 2016-12-8 14:34
"""
from job.manager import JobManager


def main():
    job_manager = JobManager()
    job_manager.start()

if __name__ == '__main__':
    main()

