import twitter, json, csv

# == OAuth Authentication ==
CONSUMER_KEY = "8waeCnUYdwfY91JC2Igac9Oiw"
CONSUMER_SECRET = "JU3FCLsX3JzqnBtcw6GM4KYasRqlGKTQ7bBbEYaKoxbn3gaLy0"
OAUTH_TOKEN = "42491024-ms2LzHwP984pji0ZltRfpPZHL7mzrQjSrWrWv7Cr1"
OAUTH_TOKEN_SECRET = "jvoh7noQED8kL294uAG6pOvqgz8NGqgiXZO2lRmnEvtmU"

auth = twitter.oauth.OAuth(OAUTH_TOKEN,
  OAUTH_TOKEN_SECRET,
  CONSUMER_KEY,
  CONSUMER_SECRET)
twitter_api = twitter.Twitter(auth=auth, retry=True)

csvfile = open('astroworld_tags.csv', 'w')
csvwriter = csv.writer(csvfile,delimiter ='|')

q = "astroworld"

# clean up our data so we can write unicode to CSV
def clean(val):
    clean = ""
    if val:
        val = val.replace('|', ' ')
        val = val.replace('\n', ' ')
        val = val.replace('\r', ' ')
        clean = val
    return clean

print ('Filtering the public timeline for keyword="%s"' % (q))
twitter_stream = twitter.TwitterStream(auth=twitter_api.auth)
stream = twitter_stream.statuses.filter(track=q)

for tweet in stream:
    # print (json.dumps(tweet, indent=1)) # for debugging purposes

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

    csvwriter.writerow([tweet['id_str'],
                        tweet['created_at'],
                        clean(tweet['user']['screen_name']),
                        clean(tweet_text),
                        tweet['user']['created_at'],
                        geo_coordinates,
                        tweet['in_reply_to_user_id_str'],
                        tweet['in_reply_to_screen_name'],
                        tweet['user']['id_str'],
                        tweet['in_reply_to_status_id_str'],
                        clean(tweet['source']),
                        tweet['user']['profile_image_url_https'],
                        tweet['user']['followers_count'],
                        tweet['user']['friends_count'],
                        tweet['user']['statuses_count'],
                        clean(tweet['user']['location'])
                        ])

    print('text: ', tweet_text) # just so we can see it runnning
