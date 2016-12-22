# encoding: utf-8
"""
redis
@author Yuriseus
@create 2016-8-29 11:26
"""
import redis

from common.decorators import fun_cost_time


class DBRedis(object):
    def __init__(self):
        self._redis_client = redis.StrictRedis(host='localhost', port=6379, db=1)

    @property
    def redis_client(self):
        return self._redis_client

    def set(self, key, value, expire=None):
        self.redis_client.set(key, value, expire)

    def get(self, key):
        return self.redis_client.get(key)

    @fun_cost_time
    def test_mget(self):
        keys = self.redis_client.keys()
        values = self.redis_client.mget(keys)
        for v in values:
            if v.decode() == 1:
                pass

    @fun_cost_time
    def test_exist(self):
        keys = self.redis_client.keys()
        for key in keys:
            if self.redis_client.exists(key):
                pass

    @fun_cost_time
    def test_pipeline(self):
        pipeline = self.redis_client.pipeline(transaction=False)
        keys = self.redis_client.keys()
        for key in keys:
            pipeline.set(key, 1)
        pipeline.execute()

db_redis = DBRedis()

if __name__ == '__main__':
    db_redis.test_pipeline()
