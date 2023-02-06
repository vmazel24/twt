from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time


#Variables that contains the user credentials to access Twitter API
consumer_key = '**********'
consumer_secret = '**********'
access_token = '**********'
access_token_secret = '**********'

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(Stream):

    def on_data(self, data):
        tweet = json.loads(data)
        try:
            if 'text' in tweet.keys():
                #print (tweet['text'])
                data = dict()
                data['created_at'] = str(tweet['created_at'])
                data['verified'] = str(tweet['user']['verified'])
                data['verified_type'] = str(tweet['user']['verified_type'])
                data['user_followers_count'] = str(tweet['user']['followers_count'])
                data['user_friends_count'] = str(tweet['user']['friends_count'])
                data['user_listed_count'] = str(tweet['user']['listed_count'])
                data['user_favourites_count'] = str(tweet['user']['favourites_count'])
                data['user_statuses_count'] = str(tweet['user']['statuses_count'])
                data['id'] = str(tweet['id'])
                data['text'] = str(tweet['text'])
                data['username'] = str(tweet['user']['name'])
                data['screenname'] = str(tweet['user']['screen_name'])
                data['tweet_quote_count'] = str(tweet['quote_count'])
                data['tweet_reply_count'] = str(tweet['reply_count'])
                data['tweet_retweet_count'] = str(tweet['retweet_count'])
                data['tweet_favorite_count'] = str(tweet['favorite_count'])

                res = client.put_record(
                    DeliveryStreamName=delivery_stream,
                    Record= {'Data': str(data)
                    }
                )
               
        except (AttributeError, Exception) as e:
                print (e)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    stream = StdOutListener(consumer_key, consumer_secret,
                              access_token, access_token_secret)

    #tweets = Table('tweets_ft',connection=conn)
    client = boto3.client('firehose', 
                          region_name='eu-west-3',
                          aws_access_key_id= '**********',
                          aws_secret_access_key= '**********' 
                          )

    delivery_stream = 'PUT-S3-kt0wN'

    while True:
        try:
            print('Twitter streaming...')
           
            stream.filter(track=['$UOS','$ATOM','$ENJ','$COTI','$SAND'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue   