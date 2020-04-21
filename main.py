import tweepy
from sqlalchemy.exc import ProgrammingError
import mysql.connector
from mysql.connector import Error


# perform in sql:
# ALTER TABLE corona CHANGE text text VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

make_corona = False
#connect to database

db = open("db.txt", "r").readlines()

try:
    connection = mysql.connector.connect(host= db[0].strip(),
                                         database=db[1].strip(),
                                         user=db[2].strip(),
                                         password=db[3].strip())
    if make_corona:
        mySql_Create_Table_Query = """CREATE TABLE corona2 ( 
                                 Id varchar(100) NOT NULL,
                                 Text varchar(1024) NULL,
                                 Name varchar(250) NULL,
                                 tweet_date Date NULL,
                                 followers int(20) NULL,
                                 friends int(20)NULL,
                                 coords varchar(250) NULL,
                                 user_created Date NULL,
                                 verified Tinyint(1) NULL,
                                 retweets int(20) NULL,                                 
                                 PRIMARY KEY (Id)) """
        cursor = connection.cursor()
        result = cursor.execute(mySql_Create_Table_Query)
        print("corona Table created successfully ")

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        #description = status.user.description
        #loc = status.user.location
        try:
            text = status.extended_tweet['full_text']
        except:
            text = status.text
        coords = status.coordinates
        #geo = status.geo
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        friends = status.user.friends_count
        verified = status.user.verified
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count
        lang = status.lang

        if lang == 'nl':
            try:
                mySql_insert_query = """INSERT INTO corona2 (Id, Text, Name, tweet_date, followers, friends, coords, user_created, verified, retweets) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Id = Id; """
                recordTuple = (id_str, str(text), name, created, int(followers), int(friends), str(coords), user_created, verified, retweets)
                cursor.execute(mySql_insert_query, recordTuple)
                connection.commit()
                print("Record inserted successfully into table")
            except ProgrammingError as err:
                print(err)
        def on_error(self, status_code):
            if status_code == 420:
                return False

def create_api_access(keys):
    auth = tweepy.OAuthHandler(keys[0].strip().encode('utf-16be').decode('utf-16'),  keys[1].strip())
    auth.set_access_token(keys[2].strip(), keys[3].strip())
    return(tweepy.API(auth))

keys = open("keys.txt", "r").readlines()

while True:
    try:
        api = create_api_access(keys)
        stream_listener = MyStreamListener()
        stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
        stream.filter(track=['lockdown'])

    except:
        continue


