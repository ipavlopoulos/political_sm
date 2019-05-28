from datetime import datetime, timedelta
import json
import os
import numpy as np
import time

import db

import pandas as pd
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')


# Credentials
key = "rM60F2NPaL3z8SK5sdkPP0ugH"
secret_key = "jmsvTYm5fYssE9qh37KfbTywgpzTiAXi3xmfItOoCH1H4XaZpG"
token = "302293972-gVUnzD6FQUtNwIzpgmkRMum07rc66EUIsXWwkiwb"
secret_token = "8gQrplfM3hPqAvUIVPhqbiw587prfSCSYcLi4ezGZhrOp"

auth = OAuthHandler(key, secret_key)
auth.set_access_token(token, secret_token)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


class listener(StreamListener):

    def __init__(self, filename):
        super(StreamListener, self).__init__()
        self.save_file = open(filename, 'w')
        self.tweets = []

    def on_data(self, data):
        print(data)
        self.tweets.append(json.loads(data))
        self.save_file.write(str(data))
        return(True)

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
        else:
            print status_code


def get_meps_ids():
    meps = pd.read_csv('gr_candidate_meps.csv')
    usernames = meps[meps['Screen Name'].str.startswith('@')]['Screen Name']
    ids = []
    for username in usernames:
        ids.append(api.get_user(screen_name=username).id_str)
    return ids


def stream_meps_tweets():
    meps_ids = get_meps_ids()
    twitterStream = Stream(auth, listener('meps_tweets.json'))
    twitterStream.filter(follow=meps_ids)


def stream_tweets_referring_meps():
    """
    Track and stream tweets that are referring to the names in the
    tracking list
    """
    meps = np.char.decode(pd.read_csv('for_tracking.csv')['Candidate'].tolist(), encoding='utf-8').tolist()
    twitterStream = Stream(auth, listener('tweets_referring_meps.json'))
    while True:
        try:
            twitterStream.filter(track=meps)
        except Exception, ex:
            print 'I just caught the exception: %s' % ex
            time.sleep(10)
            continue


def get_replies(tweet):
    user = tweet.user.screen_name
    tweet_id = tweet.id
    max_id = None
    while True:
        try:
            replies = api.search(q="to:%s" % user, since_id=tweet_id, max_id=max_id, count=100, tweet_mode="extended")
            if max_id is not None:
                # remove duplicate tweet with the id == max_id, because we got it on the previous Search
                replies = replies[1:]
        except tweepy.TweepError as e:
            print("caught twitter api error: %s", e)
            time.sleep(20)
            continue
        for reply in replies:
            if reply.in_reply_to_status_id == tweet_id:
                yield reply
                # recursively get the replies to this reply
                for reply_to_reply in get_replies(reply):
                    yield reply_to_reply
            max_id = reply.id
        if len(replies) != 100:
            break


def get_text(data):
    """
    :param data: status as json dict
    :return: the full text of the status
    """
    # Try for extended text of original tweet, if RT'd (streamer)
    try: text = data['retweeted_status']['extended_tweet']['full_text']
    except:
        # Try for extended text of an original tweet, if RT'd (REST API)
        try: text = data['retweeted_status']['full_text']
        except:
            # Try for extended text of an original tweet (streamer)
            try: text = data['extended_tweet']['full_text']
            except:
                # Try for extended text of an original tweet (REST API)
                try: text = data['full_text']
                except:
                    # Try for basic text of original tweet if RT'd
                    try: text = data['retweeted_status']['text']
                    except:
                        # Try for basic text of an original tweet
                        try: text = data['text']
                        except:
                            # Nothing left to check for
                            text = ''
    return text


def get_tweets(mps_file,
               mps_db,
               country="UK",
               context_free=False,
               start_date=datetime(2019, 2, 20, 0, 0, 0),
               end_date=datetime.now()):
    """
    retrieve the tweets of the MPs of each country
    :return:
    """
    # create json file to dump tweets
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if country == "UK":
        fname = os.path.join(dir_path, 'json_files', 'tweets_%s_to_%s.json' % (start_date, end_date))
    else: # if country == "GR"
        fname = os.path.join(dir_path, 'json_files', 'tweets_anndate_to_%s.json' % end_date)
    with open(fname, 'a'):
        os.utime(fname, None)

    # parse date columns to datetime
    dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
    # read mps csv file
    if country == "UK":
        mps = pd.read_csv(mps_file)
        politicians = mps[mps['Screen Name'].str.startswith('@')]
        users_tuples = zip(politicians['Screen Name'], politicians['Party'], [start_date] * politicians.shape[0])
    else: # if country == "GR"
        meps = pd.read_csv(mps_file, parse_dates=['Announcement date'], date_parser=dateparse)
        politicians = meps[meps['Screen Name'].str.startswith('@')]
        users_tuples = zip(politicians['Screen Name'], politicians['Party'],
                           [ann_date.to_pydatetime() for ann_date in politicians['Announcement date']])
    for username, party, start_datetime in users_tuples:
        while True:
            try:
                user = api.get_user(screen_name=username)
            except Exception, ex:
                print 'I just caught the exception: %s' % ex
                time.sleep(20)
                continue
            break

        print "Retrieve tweets from MP {} of {} party from {} to {}".format(username, party, start_datetime, end_date)
        # # if user exists in db continue
        # if db.get_user(user.id_str, mps_db): continue
        # save user in db
        db_user_id = db.get_user(user.id_str, mps_db) or db.save_user(user, mps_db, party, start_datetime, is_mp=True)

        if user.protected: continue
        statuses = []
        # retrieve most recent statuses
        max_id = None
        while True:
            while True:
                try:
                    tmp_statuses = api.user_timeline(screen_name=username, count=100, tweet_mode="extended", max_id=max_id)
                except Exception, ex:
                    print 'I just caught the exception: %s' % ex
                    time.sleep(20)
                    continue
                break

            if max_id is not None:
                # remove duplicate tweet with the id == max_id, because we got it on the previous Search
                tmp_statuses = tmp_statuses[1:]

            for status in tmp_statuses:
                # if the status date is bigger than start date append it
                if start_datetime < status.created_at < end_date:
                    # save status in db
                    status_text = get_text(status._json)
                    if not db.get_tweet(status.id_str, database):
                        db.save_tweet(status, db_user_id, mps_db, text=status_text)
                        statuses.append(status._json)
                    if not context_free:
                        # get all the replies to this tweet and the replies of the replies recursively
                        for reply in get_replies(status):
                            ruser = reply.user
                            ruser_is_mp = "@{}".format(ruser.screen_name) in politicians
                            ruser_party = mps.loc[mps['Screen Name'] == "@{}".format(ruser.screen_name)]["Party"] \
                                if ruser_is_mp else None
                            # save ruser
                            db_ruser_id = db.get_user(ruser.id_str, mps_db) or \
                                          db.save_user(ruser, mps_db, ruser_party, start_datetime, is_mp=ruser_is_mp)
                            # save the reply
                            if not db.get_tweet(reply.id_str, database):
                                print("save reply {} from user {} who replies to {}".format(reply.id, ruser.screen_name,
                                                                                            reply.in_reply_to_status_id))

                                db.save_tweet(reply, db_ruser_id, mps_db)
                                statuses.append(reply._json)

            if len(tmp_statuses) == 0 or tmp_statuses[-1].created_at < start_datetime:
                break
            else:
                max_id = tmp_statuses[-1].id

        print "Total tweets from MP {} and the replies: {}".format(username, len(statuses))
        # save tweets to json
        if len(statuses) > 0:
            with open(fname, 'a') as f:
                json.dump(statuses, f, indent=4)


def search_tweets_referring_meps(database):
    # get candidate meps screen names
    meps = pd.read_csv('gr_candidate_meps.csv')
    twitter_meps = meps[meps['Twitter username'].str.startswith('@')]['Screen Name'].str[1:]
    # find tweets mentioning the candidates accounts
    operators = ["@", "to:", "#"]
    for mep in twitter_meps:
        tweets = []
        for op in operators:
            first_attempt = True
            tmp_tweets = []
            while first_attempt or len(tmp_tweets) > 0:
                if first_attempt:
                    tmp_tweets = api.search(q=op + mep, count=100, include_entities=True, tweet_mode='extended')
                    first_attempt = False
                else:
                    tmp_tweets = api.search(q=op + mep, count=100, include_entities=True, tweet_mode='extended',
                                        max_id=tweets[-1].id)[1:]
                tweets.extend(tmp_tweets)
        print mep, len(tweets)
        for tweet in tweets:
            # save the user who wrote the tweet
            db_user_id = db.get_user(tweet.user.id_str, database) or db.save_user(tweet.user, database)
            # save the tweet
            if not db.get_tweet(tweet.id_str, database):
                db.save_tweet(tweet, db_user_id, database)


def plot_followers(database):
    def pop_std(x):
        return x.std(ddof=0)
    con = db.create_connection(database)
    df = pd.read_sql_query("SELECT * FROM user", con)
    df_followers = df.groupby(df.party, as_index=False).agg({'followers_count': ['mean', 'std', 'mad']})
    df_followers.columns = ['party', 'mean', 'std', 'mad']
    df_followers = df_followers.reindex(columns=df_followers.columns)
    df_followers.T.to_csv("followers_count.csv")

    _, ax = plt.subplots()
    ax.errorbar(df_followers['party'], df_followers['mean'], yerr=df_followers['std'], fmt='o', color='black',
                ecolor='lightgray', elinewidth=3, capsize=0)
    # ax.set_yscale('log')
    ymax = df_followers['mean'].max() + df_followers['std'].max() + 1000
    ax.set_ylim([0, ymax])
    ax.set_xlabel('Party')
    ax.set_ylabel('Followers Count')
    ax.set_title('Mean and std of followers per party')
    plt.savefig('followers_per_party.png')


if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # create databases folder if it doesn't exist
    if not os.path.exists(os.path.join(dir_path, "databases")):
        os.makedirs(os.path.join(dir_path, "databases"))
    # create json_files folder if it doesn't exist
    if not os.path.exists(os.path.join(dir_path, "json_files")):
        os.makedirs(os.path.join(dir_path, "json_files"))
    # retrieve tweets from the UK MPs
    country = "UK"
    # retrieve tweets from the GR MEPs
    # country = "GR"
    if country == "UK":
        database = os.path.join(dir_path, "databases", "twitter_with_cntx_uk.db")
        mps_file = "uk_mps.csv"
        # set start_date seven days before current datetime
        now = datetime.now()
        start_date = now - timedelta(days=7)
        # set end_date one day before current datetime to catch as much replies as possible
        end_date = now - timedelta(days=1)
        context_free = False
    else:  # if country == "GR"
        database = os.path.join(dir_path, "databases", "twitter_without_cntx_gr.db")
        mps_file = "gr_candidate_meps.csv"
        start_date = None
        context_free = True

    # create sqlite db and create tables
    if not os.path.exists(database):
        db.create_db(database)
    # create a database connection
    conn = db.create_connection(database)
    if conn is not None:
        # create twitter_user table
        db.create_table(conn, db.sql_create_user_table)
        # create tweets table
        db.create_table(conn, db.sql_create_tweet_table)
    else:
        print("Error! cannot create the database connection.")
    get_tweets(mps_file, database, country, context_free, start_date, end_date)

    # stream_tweets()
    # stream_tweets_referring_meps()
    # plot_followers(database)
    # search_tweets_referring_meps(database)