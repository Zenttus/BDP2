# tf = sc.textFile('hdfsPath')
from pyspark import SparkConf, SparkContext

df = SparkContext.read.json("/user/hdfs/tweets/02Dec2018_203919.json")
df.show()