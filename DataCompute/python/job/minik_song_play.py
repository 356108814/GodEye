# encoding: utf-8
"""
歌曲点播spark任务
@author Yuriseus
@create 16-11-07 上午10:23
"""
import sys

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings
from service.minik_server.song_play import SongPlayService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_song_play.py <date(yyyy-mm-dd)>")
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

    app_name = 'minik_song_play_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = SongPlayService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter).map(service.process_data)

    # lines_rdd.foreachPartition(service.save_to_song_play)
    # service.process_by_sql(lines_rdd, sql_context)
    service.save_to_mysql(lines_rdd)

