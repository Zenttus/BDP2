from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from hdfs import InsecureClient

import config

import sys

class TwitterStreamer():
    """
    Class for streaming tweets
    """
    def __init__(self):
        pass

    def stream_tweets(self, outputFilePath, verbose=True):
        # Twitter set up and authentication
        auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

        # Setting output
        listener = DataSaver(outputFilePath, verbose)

        stream = Stream(auth, listener)
        stream.sample()

class DataSaver(StreamListener):
    """
    Saves and prints the tweets.
    """
    def __init__(self, verbose):
        self.client_hdfs = InsecureClient(config.HDFS_SERVER)
        seld.verbose = verbose

    def on_data(self, data):
        try:
            if(self.verbose):
                print(data)
            with client_hdfs.write(config.OUTPUT_FILE_PATH, encoding = 'utf-8') as writer:
                writer(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        stream.disconnect()
        if status == 420: #Rate limit occurs
            return False

if __name__ == '__main__':

    verbose = True  # Print tweets on terminal

    if(len(sys.argv)>1):
        verbose = (sys.argv[1] == 'True')

    streamer = TwitterStreamer()
    streamer.stream_tweets(verbose)
