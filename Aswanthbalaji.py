import snscrape.modules.twitter as sntwitter
import pandas as pd
import pymongo
import streamlit as st
import datetime
import json

from pymongo import MongoClient

st.title("Twitter Scrapping Project")

Hashtag = st.text_input("Enter the HashTag")


col1, col2 = st.columns(2)
with col1:
    Start_Date = st.date_input(
    "Start Date",
    datetime.date(2022, 7, 1))
    st.write('Start Date is:', Start_Date)

with col2:
    Stop_Date = st.date_input(
    "Stop Date",
    datetime.date(2022, 11, 30))
    st.write('Stop Date is:', Stop_Date)

Stop_Date_S = str(Stop_Date)
Start_Date_S = str(Start_Date)

Twit_Limit = st.number_input('No. Of Tweets', min_value=1, max_value=1000)

Search = st.button("Search")

Store_DB = st.button("Upload")

tweets_list2 = []

if Search or Store_DB:

    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(Hashtag+' since:'+Start_Date_S+
                                    ' until:'+Stop_Date_S).get_items()):
        if i > Twit_Limit:
            break
        tweets_list2.append([tweet.date, tweet.id, tweet.content, tweet.user.username,
                             tweet.replyCount, tweet.retweetCount,
                             tweet.lang, tweet.source, tweet.likeCount])

    tweets_df2 = pd.DataFrame(tweets_list2,
                          columns=['Datetime', 'Tweet Id', 'Text', 'Username', 'ReplyCount', 'RetweetCount',
                                   'Language', 'Source', 'LikeCount'])

    st.dataframe(tweets_df2)


client = MongoClient(("mongodb://localhost:27017"))

db = client["Twitter_Data"]

if Store_DB:

    Chennai_Topic_1 = db[Hashtag]

    tweets_df2.reset_index(inplace=True)
    tweets_df2_dict = tweets_df2.to_dict("records")
    Chennai_Topic_1.insert_one({"index": Hashtag + 'data', "data": tweets_df2_dict})

    data_from_db = Chennai_Topic_1.find_one({"index":Hashtag+'data'})
    df = pd.DataFrame(data_from_db["data"])
    df.to_csv("Twitter_data.csv")
    df.to_json("Twitter_data.json")

    st.download_button("Download CSV",
                   df.to_csv(),
                   mime = 'text/csv')

    st.download_button("Download Json",
                       df.to_json(),
                       mime='json')