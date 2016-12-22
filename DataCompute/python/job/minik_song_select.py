# encoding: utf-8
"""
歌曲选择分析spark任务
@author Yuriseus
@create 16-11-8 上午11:36
"""

import sys

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings
from service.minik_server.song_select import SongSelectService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_song_select.py <date(yyyy-mm-dd)>")
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

    app_name = 'minik_song_select_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = SongSelectService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter).map(service.process_data)
    service.process_by_sql(lines_rdd, sql_context)
