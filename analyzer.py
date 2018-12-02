# tf = sc.textFile('hdfsPath')
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(_conf=conf)
sc.setLogLevel("ERROR")

df = spark.read.json("/user/hdfs/tweets/02Dec2018_203919.json")
df.show()