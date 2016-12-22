# encoding: utf-8
"""
每天定期从db库查询处理统计的。默认统计前一天
@author Yuriseus
@create 16-11-28 下午5:32
"""

import sys

from service.minik_server.user_count_metrics import UserCountMetricsService
from service.minik_server.user_active_metrics import UserActiveMetricsService

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_count_from_db.py <date(yyyy-mm-dd)>")
        exit(-1)
    date = sys.argv[1]

    # 用户数量相关统计
    ucm = UserCountMetricsService()
    ucm.update_by_date(date)

    # user active
    uam = UserActiveMetricsService()
    uam.update_by_date(date)

