# encoding: utf-8
"""
测试
@author Yuriseus
@create 16-10-25 下午4:33
"""

from pyspark import SparkConf
from pyspark import SparkContext


class Service(object):
    def __init__(self):
        self.name = 'yuri'

    def split(self, line):
        d = {}
        tmp = line.split('&')
        d['timestamp'] = tmp[0]
        d['interface'] = tmp[1]
        d['name'] = self.name
        return d


if __name__ == '__main__':
    is_debug = True
    app_name = 'test spark class'
    master_url = 'local[1]'
    conf = SparkConf().setAppName(app_name)
    if is_debug:
        conf.setMaster(master_url)
    sc = SparkContext(conf=conf)
    log_path = '/opt/data-log/minik/server_logic.log'
    service = Service()
    lines_rdd = sc.textFile(log_path).map(service.split)
    dict_list = lines_rdd.take(10)
    print(dict_list)

