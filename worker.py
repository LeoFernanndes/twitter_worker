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
import pytz


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


i = 0
while True:
    i += 1
    API_ROOT = config('API_ROOT')
    try:
        profiles_response = requests.get(f'{API_ROOT}/profiles/arrobas/')
    except:
        print(f"API did not answer on time at {datetime.datetime.now()}")
        time.sleep(60)
        continue
    print(profiles_response.content)
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
    

        
        

        """
        get most recent saved tweet
        get most recent tweet on timeline
        if tweet_date > saved_date:

        """

     



        # newer_tweet_on_timeline = api.user_timeline(screen_name=userID, 
        #                         # 200 is the maximum allowed count
        #                         count=1,
        #                         include_rts = True,
        #                         # Necessary to keep full_text 
        #                         # otherwise only the first 140 words are extracted
        #                         tweet_mode = 'extended',
        #                         )[0]

        # parsed_newer_tweet_on_timeline = parse_status(newer_tweet_on_timeline)
        # parsed_newer_tweet_on_timeline_datetime = parsed_newer_tweet_on_timeline.get('created_at')
        # if parsed_newer_tweet_on_timeline_datetime > more_recent_saved_tweet_date:
        #     print(f'{userID} segue {type(parsed_newer_tweet_on_timeline_datetime)} {parsed_newer_tweet_on_timeline_datetime} / {type(more_recent_saved_tweet_date)} {more_recent_saved_tweet_date}')
        #     print(parsed_newer_tweet_on_timeline_datetime.replace(tzinfo=pytz.utc) > more_recent_saved_tweet_date.replace(tzinfo=pytz.utc))
        # else:
        #     print(f'{userID} pula {parsed_newer_tweet_on_timeline_datetime.timestamp()} {more_recent_saved_tweet_date.timestamp()}')
        #     continue

 

        # tweets = api.user_timeline(screen_name=userID, 
        #                         # 200 is the maximum allowed count
        #                         count=200,
        #                         include_rts = True,
        #                         # Necessary to keep full_text 
        #                         # otherwise only the first 140 words are extracted
        #                         tweet_mode = 'extended',
        #                         )
        # saved_tweets = pd.read_sql(f"SELECT id_str from tweet where user_screen_name = '{userID}';", engine)
        
        # user_tweets_list = []
        # for tweet in tweets:
        #     if tweet._json.get("id_str") in set(saved_tweets['id_str']):                
        #         continue
        #     parsed_status = parse_status(tweet)
        #     user_tweets_list.append(parsed_status)
        # user_tweets_dataframe = pd.DataFrame(user_tweets_list) 
        # user_tweets_dataframe.drop_duplicates(subset="id_str", inplace=True)     
        # # user_tweets_dataframe.to_sql('tweet', con=engine, if_exists='append', index=False)  
        # user_tweets_dataframe.to_csv('/home/ubuntu/twitter_monitor/twitter-worker/local_files/tweets.csv', index=False, mode='a')    
        # saved_tweets = pd.read_sql(f"SELECT id_str from tweet where user_screen_name = '{userID}';", engine)
        # logging.info(f'{userID} - {len(set(saved_tweets.id_str))} - {datetime.datetime.now()}')


        # oldest_saved_tweet = api.user_timeline(screen_name=userID, 
        #                         # 200 is the maximum allowed count
        #                         count=1,
        #                         include_rts = True,
        #                         # Necessary to keep full_text 
        #                         # otherwise only the first 140 words are extracted
        #                         tweet_mode = 'extended',
        #                         )
        # # print(oldest_saved_tweet)

    engine.dispose()
    time.sleep(60)