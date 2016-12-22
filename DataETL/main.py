# encoding: utf-8
"""

@author Yuriseus
@create 2016-9-29 15:41
"""
from pyspark import SparkContext

logFile = "README"
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i"%(numAs, numBs))