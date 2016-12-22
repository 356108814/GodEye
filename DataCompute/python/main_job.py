# encoding: utf-8
"""
任务提交入口
@author Yuriseus
@create 16-10-25 下午4:33
"""

from pyspark import SparkConf
from pyspark import SparkContext

from common.hbase_pool import HBasePool
from util.date import DateUtil
from util.parser import ParserUtil
from service.minik_machine.user_action import UserActionSparkService


def trans_date(d):
    if 'timestamp' in d:
        date_str = d['timestamp']
        d['timestamp'] = DateUtil.date2timestamp(date_str, fmt='%y-%m-%d_%H:%M:%S')
        d['date'] = DateUtil.timestamp2date(d['timestamp'], fmt='%Y-%m-%d_%H:%M:%S')
        return d


def insert_to_user_action(action):
    hbase_client = HBasePool.get()
    cf_action = {}
    if action:
        for k, v in action.items():
            try:
                cf_action['info:%s' % k] = str(v)
            except Exception as e:
                pass
        hbase_client.put('test_user_action', str(action['timestamp']), cf_action)


if __name__ == '__main__':
    is_debug = False
    app_name = 'Minik machine user action'
    master_url = 'local[1]'
    conf = SparkConf().setAppName(app_name)
    # config.set('spark.default.parallelism', '100')
    if is_debug:
        conf.setMaster(master_url)
    sc = SparkContext(conf=conf)
    log_path = 'hdfs://CDH-0:8020/data/disk1/logdata/minik_machine/user_action/2016/2016-10-21min.log'
    # lines_rdd = sc.textFile(log_path).map(ParserUtil.split_k_v).map(trans_date)
    # 机器行为
    service = UserActionSparkService()
    service.process_by_spark(sc, log_path)

