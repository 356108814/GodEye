# encoding: utf-8
"""
游戏局spark任务
@author Yuriseus
@create 16-10-31 下午4:23
"""
import sys

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext

import config.settings
from service.minik_server.game_session import GameSessionService


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: minik_game_session.py <date(yyyy-mm-dd)>")
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

    app_name = 'minik_game_session_job_%s' % date
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    service = GameSessionService()
    lines_rdd = sc.textFile(log_path, 100).map(service.split).filter(service.filter).map(service.process_data)

    # service.process_by_sql(lines_rdd, sql_context)
    service.save_to_mysql(lines_rdd)
    # df = sql_context.createDataFrame(lines_rdd)
    # df.printSchema()
    # df.registerAsTable("session")
    #
    # dur_df = sql_context.sql("SELECT mid, SUM(durtime) AS time FROM session WHERE durtime >= 1670 GROUP BY mid")
    # for row in dur_df.collect():
    #     print(row.asDict())

    # foreachPartition若传入的是类实例的方法，则类不能使用第三方库对象，如self.db_mysql
    # lines_rdd.foreachPartition(update_to_hbase)
    # dict_list = lines_rdd.take(1)
    # print(dict_list)
