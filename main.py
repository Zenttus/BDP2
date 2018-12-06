import tweets_catcher, visualization
from tweets_catcher import HDFSManager


if __name__ == '__main__':

    #TODO spark analysis

    manager = HDFSManager()
    tweets_catcher.get_tweets(manager)
    visualization.run()