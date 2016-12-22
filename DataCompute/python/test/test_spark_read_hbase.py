# encoding: utf-8
"""
spark读取hbase
@author Yuriseus
@create 16-11-16 上午11:42
"""
import json

from pyspark import SparkContext

if __name__ == '__main__':
    host = '172.16.1.189'
    table = 'user_action'
    sc = SparkContext(appName="HBaseInputFormat")

    # Other options for configuring scan behavior are available. More information available at
    # https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/TableInputFormat.java
    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    # if len(sys.argv) > 3:
    #     conf = {"hbase.zookeeper.quorum": host, "zookeeper.znode.parent": sys.argv[3],
    #             "hbase.mapreduce.inputtable": table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)
    hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)

    output = hbase_rdd.collect()
    for (k, v) in output:
        print((k, v))

    sc.stop()
