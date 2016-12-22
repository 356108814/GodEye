# encoding: utf-8
"""

@author Yuriseus
@create 2016-8-17 15:02
"""

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from common.hbase_client_thrift import HbaseClient
from util.date import DateUtil
from util.parser import ParserUtil

hbase_client = HbaseClient("172.16.1.189", 9090)


def trans_date(d):
    if 'timestamp' in d:
        date_str = d['timestamp']
        d['timestamp'] = DateUtil.date2timestamp(date_str, fmt='%y-%m-%d_%H:%M:%S')
        d['date'] = DateUtil.timestamp2date(d['timestamp'], fmt='%Y-%m-%d_%H:%M:%S')
        return d


def insert_to_hbase(d):
    actions = d.collect()
    for action in actions:
        cf_action = {}
        for k, v in action.items():
            cf_action['info:' + k] = str(v)
        hbase_client.put('user_action', str(action['timestamp']), cf_action)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    zkQuorum, topic = sys.argv[1:]
    # kvs is pyspark.streaming.dstream.TransformedDStream
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "kafka-spark-streaming-consumer-5", {topic: 1}, {'auto.offset.reset': 'smallest'})
    lines = kvs.map(lambda x: x[1])
    data = lines.map(ParserUtil.split_k_v).map(trans_date)
    data.foreachRDD(insert_to_hbase)
    # data.saveAsTextFiles('hdfs://master:9000/data/user_action/')
    # data.repartition(1).saveAsTextFiles('hdfs://master:9000/data/user_action3/')

    ssc.start()
    ssc.awaitTermination()



