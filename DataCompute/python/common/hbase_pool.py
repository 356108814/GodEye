# encoding: utf-8
"""

@author Yuriseus
@create 16-10-25 下午5:22
"""
import random
from common.hbase_client_thrift import HbaseClient


class HBasePool(object):
    is_inited = False
    is_use1 = False
    pool = {}
    pool_size = 6

    @staticmethod
    def init():
        if not HBasePool.is_inited:
            for x in range(HBasePool.pool_size + 1):
                host = 'CDH-1'
                if x % 2 == 0:
                    host = 'CDH-2'
                HBasePool.pool[x] = {'client': HbaseClient(host, 9090), 'used': False}
            HBasePool.is_inited = True

    @staticmethod
    def get():
        HBasePool.init()

        # 返回一个未使用的
        # for k, v in HBasePool.pool.iteritems():
        #     if not v['used']:
        #         v['used'] = True
        #         return v['client']
        # 随机分一个
        # index = random.randint(1, 5)
        return HBasePool.pool[1]['client']

    @staticmethod
    def new():
        if HBasePool.is_use1:
            HBasePool.is_use1 = not HBasePool.is_use1
            return HbaseClient("CDH-2", 9090)
        else:
            HBasePool.is_use1 = not HBasePool.is_use1
            return HbaseClient("CDH-1", 9090)

    @staticmethod
    def random():
        HBasePool.init()
        index = random.randint(1, HBasePool.pool_size)
        return HBasePool.pool[index]['client']

