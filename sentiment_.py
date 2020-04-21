from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import mysql.connector
from mysql.connector import Error
import pandas as pd
import pattern
import pattern.nl
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(style="ticks", color_codes=True)
sns.set(style="darkgrid")


connection = mysql.connector.connect(host='localhost',
                                     database='tweets',
                                     user='root',
                                     password='')

mySql_select_Query = """SELECT * FROM corona """

cursor = connection.cursor()
cursor.execute(mySql_select_Query)
rows = cursor.fetchall()

connection.close()
print("MySQL connection is closed")

dat  = pd.DataFrame(rows)
dat.columns = ["tweetID", "text", "user", "date", "followers"]

senti = [pattern.nl.sentiment(i) for i in dat.text]
sentiment, subjectivity = zip(*senti)
dat = dat.assign(sent = pd.array(sentiment), pol = pd.array(subjectivity))
dat.columns


#plotting
g = sns.relplot(x="date", y="sent", kind="line", data=dat)
g.fig.autofmt_xdate()
plt.show()
#plt.savefig("mygraph.png")

dat_small = dat[dat.followers < 100]
p = sns.relplot(x="followers", y="sent", data=dat_small);
plt.show()

dat.sent.mean()
dat.pol.mean()

sns.distplot(dat.sent, kde=False);
sns.distplot(dat.pol);

'''
# works only for english texts. Option would be to translate everything first.
vs = []
analyzer = SentimentIntensityAnalyzer()
for sentence in dat.text:
    vs.append(analyzer.polarity_scores(sentence))

'''



