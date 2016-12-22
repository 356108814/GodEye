# encoding: utf-8
"""
minik用户行为分析spark任务
@author Yuriseus
@create 16-11-8 下午17:36
"""

import sys
from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings
from service.minik_machine.user_action import UserActionService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_user_action.py <date(yyyy-mm-dd)>")
        exit(-1)

    master_url = 'local[1]'
    date = ''
    conf = SparkConf()
    if config.settings.DEBUG:
        conf.setMaster(master_url)
        log_path = '/opt/data-log/minik/machine_user_action.log'
    else:
        date = sys.argv[1]
        year = date[0:4]
        log_path = 'hdfs://CDH-0:8020/data/disk1/logdata/minik_machine/user_action/%s/%s.log' % (year, date)

    app_name = 'minik_user_action_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = UserActionService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter).map(service.process_data)
    service.process_by_sql(lines_rdd, sql_context)
    # 流水保存到mysql数据库
    lines_rdd = lines_rdd.map(service.get_column_value)
    service.save_to_mysql_user_action(lines_rdd)
