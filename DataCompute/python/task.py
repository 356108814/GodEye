# encoding: utf-8
"""
任务
@author Yuriseus
@create 16-10-24 下午1:52
"""
import time
from service.minik_machine.user_action import UserActionService


if __name__ == '__main__':
    start_time = time.time()
    file_path = '/opt/data-log/2016-10-20.log'
    user_action_service = UserActionService()
    user_action_service.start(file_path)
    print('time %s second' % (time.time() - start_time))

