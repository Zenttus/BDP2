# tf = sc.textFile('hdfsPath')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

#def save_data(file, data ):
    # Hastags
    # Words
    # Users
    # Keywords


def run():
    ss = SparkSession.builder.getOrCreate()
    df = ss.read.json("C:\\Users\\Owrn\\Documents\\gitRepos\BDP2\\temp.json")
    df.first()