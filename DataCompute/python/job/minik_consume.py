# encoding: utf-8
"""
minik歌曲点播spark任务
@author Yuriseus
@create 16-11-11 下午3:17
"""
import sys

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings
from service.minik_server.consume import ConsumeService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_consume.py <date(yyyy-mm-dd)>")
        exit(-1)

    master_url = 'local[1]'
    date = ''
    conf = SparkConf()
    if config.settings.DEBUG:
        conf.setMaster(master_url)
        log_path = '/opt/data-log/minik/server_logic.log'
    else:
        date = sys.argv[1]
        year = date[0:4]
        log_path = 'hdfs://CDH-0:8020/data/disk1/logdata/minik_server/logic/%s/%s.log' % (year, date)

    app_name = 'minik_consume_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = ConsumeService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter).map(service.process_data)
    lines_rdd.cache()
    service.save_to_consume(lines_rdd)
    service.process_by_sql(lines_rdd, sql_context)


