import sys
import tweets_catcher
from hdfs_manager import HDFSManager
#TODO get modules

if __name__ == '__main__':

    #TODO spark analysis
    #TODO gui

    manager = HDFSManager()

    tweets_catcher.get_tweets(manager)
