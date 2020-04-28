import tweepy
from sqlalchemy.exc import ProgrammingError
import mysql.connector
from mysql.connector import Error



# make a txt file containing db information
db = open("db.txt", "r").readlines()
# make a keys.txt file to read in twitter API credentials
keys = open("keys2.txt", "r").readlines()
#define table name here
tab_name = 'test'
# provide keyword or keywords you want to track
keyword = 'lockdown'

# creating and/or connecting to our database
try:
    connection = mysql.connector.connect(host= db[0].strip(),
                                         database=db[1].strip(),
                                         user=db[2].strip(),
                                         password=db[3].strip())
    cursor = connection.cursor(buffered=True)

    mySql_Create_Table_Query = "CREATE TABLE IF NOT EXISTS " + tab_name + """
                            (Id varchar(100) NOT NULL,
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
    cursor.execute(mySql_Create_Table_Query)
    print("Table created successfully ")
    # making sure twitter data fits our table using the alter statement
    alterStatement = "ALTER TABLE " + tab_name + " CHANGE text text VARCHAR(1024) CHARACTER" \
                     " SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    cursor.execute(alterStatement)

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
    table_name = ''

    def on_status(self, status):
        # collect status information during listening
        # we only capture the information we need for each tweet
        try:
            # try to get extended tweet, else regular tweet
            text = status.extended_tweet['full_text']
        except:
            text = status.text
        coords = status.coordinates
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        friends = status.user.friends_count
        verified = status.user.verified
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count
        lang = status.lang
        # insert into database only if the language is dutch
        if lang == 'nl':
            try:
                mysql_insert_query = "INSERT INTO " + self.table_name + """ (Id, Text, Name, tweet_date, followers, friends, coords, user_created, verified, retweets) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Id = Id; """
                recordtuple = (id_str, str(text), name, created, int(followers), int(friends), str(coords), user_created, verified, retweets)
                cursor.execute(mysql_insert_query, recordtuple)
                connection.commit()
                print("Record inserted successfully into table")
            except ProgrammingError as err:
                print(err)

        def on_error(status_code):
            if status_code == 420:
                return False

    def create_api_access(self, keys):
        auth = tweepy.OAuthHandler(keys[0].strip(), keys[1].strip())
        auth.set_access_token(keys[2].strip(), keys[3].strip())
        self.api = tweepy.API(auth)

while True:
    try:
        stream_listener = MyStreamListener()
        stream_listener.create_api_access(keys)
        stream_listener.table_name = tab_name
        stream = tweepy.Stream(auth=stream_listener.api.auth, listener=stream_listener)
        stream.filter(track=[keyword])
    except:
        continue


