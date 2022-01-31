import datetime
import requests
from decouple import config
import time
from twitter import get_more_recent_saved_tweet_date, fist_profile_tweet_load, parse_status, twitter_authentication
import json
from database import SQLALCHEMY_DATABASE_URL
from sqlalchemy import create_engine
import pandas as pd
import logging


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":

    i = 0
    while True:
        i += 1
        API_URL = config('API_URL')
        try:
            endpoint = f'{API_URL}/profiles/arrobas/'
            profiles_response = requests.get(endpoint)
        except:
            print(f"API {endpoint} did not answer on time at {datetime.datetime.now()}")
            time.sleep(60)
            continue
        if profiles_response.status_code != 200:
            print(profiles_response.content)
            time.sleep(60)
            continue
        
        parsed_profiles_response = json.loads(profiles_response.content)
        api = twitter_authentication()
        engine = create_engine(SQLALCHEMY_DATABASE_URL)   
        for userID in parsed_profiles_response:   
            more_recent_saved_tweet_date = get_more_recent_saved_tweet_date(userID, engine)
            saved_tweets_rows_count = pd.read_sql(f"SELECT id_str from tweet where user_screen_name = '{userID}';", engine).shape[0]
            if saved_tweets_rows_count == 0:
                fist_profile_tweet_load(userID, engine)
            else:
                most_recent_saved_tweets = pd.read_sql(f"SELECT id_str, created_at from tweet where user_screen_name = '{userID}' order by created_at DESC;", engine)
                most_recent_saved_tweet_id = most_recent_saved_tweets['id_str'][0]
                print(userID, most_recent_saved_tweets.loc[0])
                newer_tweets_on_timeline = api.user_timeline(screen_name=userID, 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = True,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended',
                            since_id=int(most_recent_saved_tweet_id)
                            )
                for tweet in newer_tweets_on_timeline:
                    parsed_tweet = parse_status(tweet)
                    print(parsed_tweet.get('id_str'), parsed_tweet.get('created_at'), len(newer_tweets_on_timeline))
                if len(newer_tweets_on_timeline) == 0:
                    print(f"No new tweets from {userID} at {datetime.datetime.now()}")
        engine.dispose()
        print(f">>> Execution {datetime.datetime.now()} <<<")
        time.sleep(60)