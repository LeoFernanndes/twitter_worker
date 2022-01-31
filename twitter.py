from datetime import datetime
from sqlalchemy import engine
import tweepy as tw
from decouple import config
from tweepy.models import Status
import pandas as pd
import pytz

def twitter_authentication():
    auth = tw.OAuthHandler(consumer_key=config('TWITTER_API_KEY'), consumer_secret=config('TWITTER_API_KEY_SECRET'))
    auth.set_access_token(config('TWITTER_ACCESS_TOKEN'), config('TWITTER_ACCESS_TOKEN_SECRET'))
    api = tw.API(auth, wait_on_rate_limit=True)
    return api

def parse_status(status: Status):
    parsed_status = {
        "id_str": status._json.get('id_str'),
        "user_screen_name": status._json.get('user').get('screen_name'),
        "user_id_str": status._json.get('user').get('id_str'),
        "full_text" :status._json.get('full_text'),
        "truncated": status._json.get('truncated'),
        "created_at": status.created_at.replace(tzinfo=pytz.utc),
        "entities_hashtags": status._json.get('entities_hashtags'),
        "entities_symbols": status._json.get('entities_symbols'),
        "entities_user_mentions": status._json.get('entities_user_mentions'),
        "entities_urls": status._json.get('entities_urls'),
        "geo": status._json.get('geo'),
        # "coordinates": status._json.get('coordinates'),
        "coordinates": "pass",
        "place": status._json.get('place'),
        "contributors": status._json.get('contributors'),
        "is_quote_status": status._json.get('is_quote_status'),
        "favorite_count": status._json.get('favorite_count'),
        "favorited": status._json.get('favorited'),
        "retweeted": status._json.get('retweeted'),
        "possibly_sensitive": status._json.get('possibly_sensitive'),
        "lang": status._json.get('lang')
    }
    return parsed_status

def get_more_recent_saved_tweet_date(user_id: str, db_engine: engine.Engine) -> datetime:
    try:                
        more_recent_saved_tweet_date = pd.read_sql(f"SELECT created_at from tweet where user_screen_name = '{user_id}' order by created_at DESC;", db_engine)[0]
    except KeyError:
        more_recent_saved_tweet_date = datetime(2000, 1, 1)    
    return more_recent_saved_tweet_date.replace(tzinfo=pytz.utc)

def fist_profile_tweet_load(user_id: str, db_engine: engine.Engine) -> None:
    api = twitter_authentication()
    tweet_list = []
    len_retrieved_tweets = 201
    newer_tweet_on_timeline = api.user_timeline(screen_name=user_id, 
                            # 200 is the maximum allowed count
                            count=1,
                            include_rts = True,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )[0]

    parsed_newer_tweet_on_timeline = parse_status(newer_tweet_on_timeline)                        
    older_saved_tweet_id = parsed_newer_tweet_on_timeline.get('id_str')
    print(f"First load start for {user_id}")
    while len_retrieved_tweets > 100:
        retrieved_tweets = api.user_timeline(screen_name=user_id, 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = True,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended',
                            max_id=int(older_saved_tweet_id)
                            )            
    
        for tweet in retrieved_tweets:
            tweet_list.append(parse_status(tweet))
        older_saved_tweet_id = parse_status(retrieved_tweets[-1]).get('id_str')
        len_retrieved_tweets = len(retrieved_tweets)     

    total_dataframe = pd.DataFrame(tweet_list)
    total_dataframe.drop_duplicates().to_sql('tweet', con=db_engine, if_exists='append', index=False)  
    total_dataframe.to_sql('tweet', con=db_engine, if_exists='append', index=False)  
    print(f"{user_id} successfully loaded {total_dataframe.drop_duplicates().shape[0]} rows")
