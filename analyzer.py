# tf = sc.textFile('hdfsPath')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()

df = spark.read.json("/user/hdfs/tweets/02Dec2018_203919.json")
df.show()