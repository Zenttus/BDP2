import tweets_catcher
from tweets_catcher import HDFSManager


if __name__ == '__main__':

    manager = HDFSManager()
    tweets_catcher.get_tweets(manager)