# tf = sc.textFile('hdfsPath')
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
# create spark configuration
c = SparkConf()
c.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=c)
sc.setLogLevel("ERROR")

df = spark.read.json("/user/hdfs/tweets/02Dec2018_203919.json")
df.show()