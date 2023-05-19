import requests
import os
import json
import csv
import twitter
from datetime import datetime
import time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

search_url = "https://api.twitter.com/2/tweets/search/all"

csvFile = open('astroworld_tweets_week_satanic.csv', 'w')
csvWriter = csv.writer(csvFile,delimiter ='|')

csvWriter.writerow(['author id', 'created_at', 'geo', 'id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count','source','tweet'])

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = "Bearer {}".format(bearer_token)
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r



def clean(val):
    clean = ""
    if val:
        val = val.replace('|', ' ')
        val = val.replace('\n', ' ')
        val = val.replace('\r', ' ')
        clean = val
    return clean

def connect_to_endpoint(url, params):
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

starttime = datetime.strptime('2021-11-05T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')
endtime = datetime.strptime('2021-11-05T01:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')

startday = 7

while startday < 13:
    starttime = starttime.replace(day=startday)
    endtime = endtime.replace(day=startday)
    startday += 1
    starthour = 0
    endhour = 1
    while starthour < 23:
        starttime = starttime.replace(hour=starthour)
        endtime = endtime.replace(hour=endhour)
        formattedendtime = endtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        formattedstarttime = starttime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        starthour += 1
        endhour += 1

        query_params = {'query': 'astroworld satanic OR astroworld ritual', 'start_time': formattedstarttime, "end_time": formattedendtime, 'max_results': 500, 'expansions': 'author_id,in_reply_to_user_id,geo.place_id', 'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source', 'user.fields': 'id,name,username,created_at,description,public_metrics,verified', 'place.fields': 'full_name,id,country,country_code,geo,name,place_type',}
        json_response = connect_to_endpoint(search_url, query_params)
        print(json.dumps(json_response, indent=4, sort_keys=True))


        for tweet in json_response['data']:
            print (json.dumps(tweet, indent=1)) # for debugging purposes

            if 'truncated' in tweet:
                if tweet['truncated']:
                    tweet_text = tweet['extended_tweet']['full_text']
                else:
                    tweet_text = tweet['text']

            if 'retweeted_status' in tweet:
                rt_prefix = 'RT @' + tweet['retweeted_status']['user']['screen_name'] + ': '
                if tweet['retweeted_status']['truncated']:
                    tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
                else:
                    tweet_text = tweet['retweeted_status']['text']
                tweet_text = rt_prefix +  tweet_text
                if ('quoted_status' in tweet):
                    quote_suffix =  tweet['quoted_status_permalink']['url']
                    tweet_text = tweet_text + ' ' + quote_suffix

            if ('quoted_status' in tweet) and ('retweeted_status' not in tweet) :
                quote_suffix =  tweet['quoted_status_permalink']['url']
                tweet_text = tweet_text + ' ' + quote_suffix

            geo_coordinates = ""
            if 'coordinates' in tweet:
                if tweet['coordinates']:
                    geo_coordinates = tweet['coordinates']['coordinates'][1] + ',' + tweet['coordinates']['coordinates'][0]

            # 1. Author ID
            author_id = tweet['author_id']

            # 2. Time created
            created_at = tweet['created_at']

            # 3. Geolocation
            if ('geo' in tweet):
                geo = tweet['geo']['place_id']
            else:
                geo = " "

            # 4. Tweet ID
            tweet_id = tweet['id']

            # 5. Language
            lang = tweet['lang']

            # 6. Tweet metrics
            retweet_count = tweet['public_metrics']['retweet_count']
            reply_count = tweet['public_metrics']['reply_count']
            like_count = tweet['public_metrics']['like_count']
            quote_count = tweet['public_metrics']['quote_count']

            # 7. source
            source = tweet['source']

            # 8. Tweet text
            text = clean(tweet['text'])

            # Assemble all data in a list
            res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source, text]

            # Append the result to the CSV file
            csvWriter.writerow(res)

        time.sleep(5)
