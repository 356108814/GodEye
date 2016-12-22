# encoding: utf-8
"""
minik机器用户行为job
@author Yuriseus
@create 16-10-21 下午4:33
"""

from pyspark import SparkConf
from pyspark import SparkContext

from common.hbase_pool import HBasePool
from util.date import DateUtil
from util.parser import ParserUtil

# hbase_client = HbaseClient("172.16.1.189", 9090)


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
    if is_debug:
        conf.setMaster(master_url)
    sc = SparkContext(conf=conf)
    log_path = 'hdfs://CDH-0:8020/data/disk1/logdata/minik_machine/user_action/2016/2016-10-21.log'
    # log_path = 'hdfs://master:9000/data/machine_user_action.log'
    lines = sc.textFile(log_path).map(ParserUtil.split_k_v).map(trans_date)
    # actions = lines.collect()    # 从集群上拉取所以数据到本地，可能导致内存溢出，foreach还不知道怎么用

    # 机器行为
    # service = MinikMachineUserActionService()
    # service.insert_to_user_action(actions)
    lines.foreach(insert_to_user_action)

