# encoding: utf-8
"""

@author Yuriseus
@create 2016-10-8 17:05
"""
import time, setting
from importlib import reload


if __name__ == '__main__':
    # while True:
    #     while True:
    #         print('in')
    #         time.sleep(2)
    #     reload(setting)
    #     if setting.IS_RUNNING:
    #         print('running')
    #     else:
    #         print('stop')
    #     time.sleep(1)
    date = '2016-09-09'
    print(date[0:4])
    print(type(''))

    while True:
        try:
            open('aaaa')
            print(111)
        except Exception as e:
            print('err')
            pass

    # with open('logs/sincedb', 'a+') as f:
    #     for x in range(100000):
    #         f.write('[I 161009 16:23:07 log:109] merge: /diskb/aimei_data_log/minik_server/logic_log2/2016/2016-10-09/121.201.48.59/2016100915561542751\n')
