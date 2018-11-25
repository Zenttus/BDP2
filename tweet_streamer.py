from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from hdfs import InsecureClient

import config

import sys
import time
import subprocess
from time import strftime, gmtime

class TwitterStreamer():
    """
    Class for streaming tweets
    """
    def __init__(self):
        pass

    def stream_tweets(self, verbose=True):
        # Twitter set up and authentication
        auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

        # Setting output
        listener = DataSaver(verbose)

        stream = Stream(auth, listener)
        stream.sample()

class DataSaver(StreamListener):
    """
    Saves the tweets in intervals.
    """
    def __init__(self, verbose):
        self.verbose = verbose

        # Start comunication with HDFS
        self.client_hdfs = InsecureClient(config.HDFS_SERVER)

        # Start tracking time
        self.tick = time.clock()

        # Create new file for tweets
        self.files = [ config.OUTPUT_FILE_PATH + strftime("%d%b%Y%H:%M:%S", gmtime()) + ".json" ]
        put = subprocess.Popen(["hdfs", "dfs", "-touchz", self.files[-1]])
        put.communicate()

        # Creates list to keep track of files
        self.fileList = open(config.LIST_PATH + "tweetsList.txt", "a+")
        self.fileList.write(self.files[-1] + "\n")

    def on_data(self, data):

        if(self.tick-time.clock() > config.INTERVAL):
            # Time interval completed, reseting and createing a new file
            self.tick = time.clock()
            self.files.append(config.OUTPUT_FILE_PATH + strftime("%d%b%Y%H:%M:%S", gmtime()) + ".json")
            put = subprocess.Popen(["hdfs", "dfs", "-touchz", self.files[-1]], stdin=cat.stdout)
            put.communicate()
            self.fileList.write(self.files[-1] + "\n") #update file list

        try:
            if(self.verbose):
                print(data)
            self.client_hdfs.write(self.files[-1], data = data, append=True, encoding = 'utf-8')
            return True

        except BaseException as e:
            print("Error on_data %s" % str(e))
            self.fileList.close()

        return True

    def on_error(self, status):
        print(status)
        stream.disconnect()
        self.fileList.close()
        if status == 420: #Rate limit occurs
            return False



if __name__ == '__main__':

    #TODO spark analysis
    #TODO delete files
    #TODO gui
    verbose = True  # Print tweets on terminal

    if(len(sys.argv)>1):
        verbose = (sys.argv[1] == 'True')

    streamer = TwitterStreamer()
    streamer.stream_tweets(verbose)
