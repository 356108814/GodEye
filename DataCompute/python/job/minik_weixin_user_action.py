# encoding: utf-8
"""
minik用户行为分析spark任务
@author Yuriseus
@create 16-12-7 上午11:36
"""

import sys

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings as settings
from service.minik_weixin.user_action import UserActionService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_weixin_user_action.py <date(yyyy-mm-dd)>")
        exit(-1)

    master_url = 'local[1]'
    date = ''
    conf = SparkConf()
    if settings.DEBUG:
        conf.setMaster(master_url)
        log_path = '/opt/data-log/minik/weixin_user_action.log'
    else:
        date = sys.argv[1]
        year = date[0:4]
        log_path = 'hdfs://CDH-0:8020/data/disk1/logdata/minik_weixin/user_action/%s/%s.log' % (year, date)

    app_name = 'minik_weixin_user_action_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = UserActionService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter)

    # 流水保存到mysql数据库
    rdd1 = lines_rdd.map(service.get_column_value)
    service.save_to_mysql_user_action(rdd1)

    # 统计点击
    rdd2 = lines_rdd.map(service.process_data)
    service.count_click_metrics(rdd2, sql_context)
