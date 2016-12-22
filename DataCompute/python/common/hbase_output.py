# encoding: utf-8
"""
spark 写hbase
@author Yuriseus
@create 16-10-26 下午7:39
"""
from pyspark import SparkContext


class HbaseOutput(object):
    def __init__(self, host='CDH-0:9090', table='user_action'):
        self.conf = {"hbase.zookeeper.quorum": host,
                     "hbase.mapred.outputtable": table,
                     "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
                     "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                     "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
        self.keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
        self.valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    def put(self, rdd):
        rdd.saveAsNewAPIHadoopDataset(
            conf=self.conf,
            # keyConverter=self.keyConv,
            # valueConverter=self.valueConv
        )

if __name__ == '__main__':
    sc = SparkContext(appName="HBaseOutputFormat")
    rdd = sc.parallelize(['yuri']).map(lambda x: ('f:name', x))
    r = rdd.collect()
    print(r)
    output = HbaseOutput(table='f')
    output.put(rdd)
