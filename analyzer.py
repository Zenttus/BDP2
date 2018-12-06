# tf = sc.textFile('hdfsPath')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

ss = SparkSession.builder.getOrCreate()
df = sa.read.json("C:\\Users\\Owrn\\Documents\\gitRepos\BDP2\\temp.json")
df.first()