from os.path import dirname, abspath
from datetime import datetime
import json
import sqlite3
from sqlite3 import Error


sql_create_user_table = """create table if not exists user (
    id integer primary key autoincrement,
    user_id_str text not null,
    name text,
    screen_name text not null,
    party text,
    announcement_date datetime,
    is_mp boolean not null,
    location text,
    url text,
    description text,
    protected boolean,
    verified boolean,
    followers_count integer not null,
    friends_count integer not null,
    listed_count integer,
    favourites_count integer,
    statuses_count integer not null,
    created_at datetime,
    lang text,
    contributors_enabled boolean,
    profile_background_color text,
    profile_background_image_url text,
    profile_image_url text
);"""


sql_create_tweet_table = """create table if not exists tweet (
    id integer primary key autoincrement,
    tweet_id_str text not null,
    full_text text not null,
    in_reply_to_status_id integer,
    in_reply_to_user_id integer,
    source text,
    is_quote_status boolean,
    quoted_status_id_str text,
    retweeted_status_id_str text,
    quote_count integer,
    retweet_count integer,
    favorite_count integer,
    entities text,
    extended_entities text,
    created_at datetime not null,
    possibly_sensitive boolean,
    lang text,
    user_id INTEGER,
        FOREIGN KEY(user_id) REFERENCES user(id)
);"""


def create_db(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    finally:
        conn.close()


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def str_to_datetime(date_time_str):
    return datetime.strptime(date_time_str, '%a %b %d %H:%M:%S +0000 %Y')


def save_user(user, database, party=None, ann_date=None, is_mp=False):
    twitter_user = (user.id_str, user.name, user.screen_name, party, ann_date, is_mp, user.location, user.url,
                    user.description, user.protected, user.verified, user.followers_count, user.friends_count,
                    user.listed_count, user.favourites_count, user.statuses_count, user.created_at,
                    user.lang, user.contributors_enabled, user.profile_background_color,
                    user.profile_background_image_url, user.profile_image_url)
    with sqlite3.connect(database) as con:
        cur = con.cursor()
        sql = "INSERT INTO user (user_id_str,name,screen_name,party,announcement_date,is_mp, location,url," \
              "description,protected,verified,followers_count,friends_count,listed_count,favourites_count," \
              "statuses_count,created_at,lang,contributors_enabled,profile_background_color," \
              "profile_background_image_url,profile_image_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        cur.execute(sql, twitter_user)
        return cur.lastrowid


def save_tweet(status, db_user_id, database, text=''):
    retweeted_status = getattr(status, "retweeted_status", None)
    if retweeted_status: retweeted_status_id_str = retweeted_status.id_str
    else: retweeted_status_id_str = None
    status_text = text or status.full_text
    tweet = (status.id_str, status_text, status.in_reply_to_status_id, status.in_reply_to_user_id, status.source,
             status.is_quote_status, retweeted_status_id_str, getattr(status, "quote_count", None),
             status.retweet_count, getattr(status, "favorite_count", None), json.dumps(getattr(status, "entities", None)),
             json.dumps(getattr(status, "extended_entities", None)), status.created_at,
             getattr(status, "possibly_sensitive", None), getattr(status, "lang", None), db_user_id)
    with sqlite3.connect(database) as con:
        cur = con.cursor()
        sql = "INSERT INTO tweet (tweet_id_str, full_text, in_reply_to_status_id," \
              "in_reply_to_user_id, source, is_quote_status, retweeted_status_id_str, quote_count, " \
              "retweet_count, favorite_count, entities, extended_entities, created_at," \
              "possibly_sensitive, lang, user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        cur.execute(sql, tweet)
        return cur.lastrowid


def get_user(user_id, database):
    with sqlite3.connect(database) as con:
        cur = con.cursor()
        cur.execute("SELECT id FROM user WHERE user_id_str=?", (user_id,))
        result = cur.fetchone()
        if result:
            return result[0]
        return result


def get_tweet(tweet_id, database):
    with sqlite3.connect(database) as con:
        cur = con.cursor()
        cur.execute("SELECT id FROM tweet WHERE tweet_id_str=?", (tweet_id,))
        result = cur.fetchone()
        if result:
            return result[0]
        return result