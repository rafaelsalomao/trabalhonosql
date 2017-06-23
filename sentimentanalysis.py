import re
import tweepy
from pymongo import MongoClient
from textblob import TextBlob
from decouple import config
from tweepy import Stream
from tweepy import StreamListener
from tweepy.streaming import json

CONSUMER_KEY = 'Uu1b1DZLoMWbQcMyWKGwZSev9'
CONSUMER_SECRET = 'S8XEadEfHwfVG1CsSRoDj4pILuuCq6eUxielTjeIoqD55z9Txr'
ACCESS_TOKEN = '756498485172633600-gSU6TGY64PZedYLjamN8eW54YNHP6v3'
ACCESS_TOKEN_SECRET = 'kqdB7e25nmI4dGxp2Qsj9H4gOKuy2fzQvfZlcrfd4Tvha'

client = MongoClient('localhost', 27017)
db = client.aula


class TweetStreamListener(StreamListener):

    def on_data(self, data):

        tweet = json.loads(data)

        filtered_tweet = {}

        try:
            # Clean tweet text (Remove links and special characters)
            filtered_tweet["text"] = ' '.join(
                re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)",
                       " ", tweet["text"]).split())

            # Sentiment Analysis
            analysis = TextBlob(filtered_tweet["text"])
            if analysis.sentiment.polarity > 0:
                filtered_tweet["sentiment"] = "positive"
            elif analysis.sentiment.polarity == 0:
                filtered_tweet["sentiment"] = "neutral"
            else:
                filtered_tweet["sentiment"] = "negative"

            # Ignore retweets
            if (not tweet["retweeted"]) and ('RT @' not in tweet["text"]):
                db.tweets.insert(filtered_tweet)
        except KeyError:
            pass

if __name__ == '__main__':
    listener = TweetStreamListener()

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)
    stream.filter(track=['kalil'], async=True)
