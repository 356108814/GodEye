# encoding: utf-8
"""

@author Yuriseus
@create 16-11-18 上午10:35
"""
import sys
import time

import datetime

from etl.minik_server import MinikServerETL
from etl.minik_machine import MinikMachineETL
from util.date import DateUtil

if __name__ == '__main__':
    # if len(sys.argv) != 2:
    #     print("Usage: start_etl.py <date(yyyy-mm-dd)>")
    #     exit(-1)

    start_time = time.time()
    date = sys.argv[1]
    if not date:
        # 默认前一天
        date = DateUtil.get_interval_day_date(datetime.datetime.now(), -1)
    print('etl minik machine log %s' % date)

    etl = MinikMachineETL()
    etl.process_start(date)

    print('etl minik server log %s' % date)
    etl = MinikServerETL()
    etl.process_start(date)

    print('cost time %s second' % (time.time() - start_time))

