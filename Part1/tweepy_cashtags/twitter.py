import tweepy
import secret_info as s
from Mystream import Mystream
import time

#client = tweepy.Client(s.bearer_token,s.api_key,s.api_secret,s.access_token,s.access_secret)

#auth = tweepy.OAuth1UserHandler(s.api_key,s.api_secret)
# auth.set_access_token(s.access_token,s.access_secret)

#api = tweepy.API(auth)

streaming_client = Mystream(s.bearer_token)


streaming_client.filter(expansions=['author_id', 'geo.place_id', 'referenced_tweets.id'], place_fields=['full_name', 'country'], tweet_fields=[
                        'id', 'text', 'public_metrics', 'created_at', 'non_public_metrics', 'conversation_id'], user_fields=['created_at', 'public_metrics', 'verified'])

# [author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,edit_history_tweet_ids]
