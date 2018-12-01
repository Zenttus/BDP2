from twitter import OAuth, TwitterStream
from hdfs_manager import HDFSManager
import config


def get_tweets(hdfsmanager):
    assert hdfsmanager.__class__ == HDFSManager

    authorization = OAuth(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET, config.CONSUMER_KEY, config.CONSUMER_SECRET)

    stream = TwitterStream(auth=authorization)

    tweets = stream.statuses.sample()

    for tweet in tweets:
        try:
            if len(tweet) > 2:  #This filters the "deleted" tweets
                hdfsmanager.save_tweet(tweet)
        except Exception as e:
            print(e)
            pass