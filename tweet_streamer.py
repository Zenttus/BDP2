from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# TODO delete files

import config

import sys
import time
import subprocess
from time import strftime, gmtime


class TwitterStreamer:
    """
    Class for streaming tweets
    """
    @staticmethod
    def stream_tweets(ver=True):
        # Authentication
        auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

        # Setting output
        listener = DataSaver(ver)

        stream = Stream(auth, listener, stall_warnings=True, _async=True, filter_level='medium')

        try:
            while True:
                try:
                    stream.sample()
                    time.sleep(config.INTERVAL) #halts the control for runtime seconds
                except KeyboardInterrupt:
                    print('KEYBOARD INTERRUPT')
                    return
        finally:
            stream.disconnect()


class DataSaver(StreamListener):
    """
    Saves the tweets in intervals.
    """
    def __init__(self, ver):
        self.verbose = ver

        # Start communication with HDFS
        #self.client_hdfs = InsecureClient(config.HDFS_SERVER)

        # Create new file for tweets
        #self.files = [ config.OUTPUT_FILE_PATH + strftime("%d%b%Y_%H%M%S", gmtime()) + ".json" ]

       ### put = subprocess.Popen(["hdfs dfs -touchz " + self.files[-1]], shell=True)
       # put.communicate()

        # Creates list to keep track of files
       # self.fileList = open(config.LIST_PATH + "tweetsList.txt", "a+")
       # self.fileList.write(self.files[-1] + "\n")
        self.tempFile = open("./temp.json", "a+")

    def send_tweets_hdfs(self):
        self.fileList.write(self.files[-1] + "\n") #update file list

        self.files.append(config.OUTPUT_FILE_PATH + strftime("%d%b%Y%H:%M:%S", gmtime()) + ".json")
        self.tempFile.close()
        put = subprocess.Popen(["hdfs", "hadoop", "fs," "-put", "./temp.json", self.files[-1]], shell=True)
        put.communicate()

    def on_data(self, data):

        try:
            if self.verbose:
                print(data)
            #self.tempFile.write(data)
            #self.client_hdfs.write(self.files[-1], data = data, append=True, encoding = 'utf-8')
            return True

        except BaseException as e:
            print("Error on_data %s" % str(e))
            self.fileList.close()

        return True

    def on_error(self, status):
        print(status)
        #self.fileList.close()
        if status == 420: #Rate limit occurs
            return False
    
    def on_exception(self,e):
        print(e)
        return


