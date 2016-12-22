# encoding: utf-8
"""
项目配置
@author Yuriseus
@create 2016-8-3 11:34
"""
import os

DEBUG = False

DEFAULT_CON_PATH = 'default.config'

# 包含字典的字典，解析自DEFAULT_CON_PATH
CONF = {}

# 当该标志设为False时，全部服务服务处理完后退出。用于当插入数据失败时，终止运行，减少数据丢失
GLOBAL_RUNNING = True

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')

# 日报收件人
DAILY_REPORT_TO_MAILS = ['769435570@qq.com']


if __name__ == '__main__':
    print(BASE_DIR)
