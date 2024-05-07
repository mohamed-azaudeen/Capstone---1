from googleapiclient.discovery import build
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st

def Api_connect():
    api_key = "AIzaSyAMF2dShEm4C77UVYPMhhO5xx2DauOcU3E"

    api_service_name = "youtube"
    api_version = "v3"

    youtube =build(api_service_name, api_version, developerKey=api_key)

    return youtube

youtube=Api_connect()

def channel_ids(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    data = { "channel_name":response['items'][0]['snippet']['title'],
            "channel_publishedOn":response['items'][0]['snippet']['publishedAt'],
            "channel_id":response['items'][0]['id'],
            "channel_description":response['items'][0]['snippet']['description'],
            "channel_subscriptionCount":response['items'][0]['statistics']['subscriberCount'],
            "channel_viewCount":response['items'][0]['statistics']['viewCount'],
            "channel_videoCount":response['items'][0]['statistics']['videoCount'] 
            }
    return data

def video_ids(channel_id):
    video_id = []

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    playlist_id =  response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token )
                
        response1 = request.execute()

        for i in range(len(response1['items'])):
            video_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken') 

        if next_page_token is None :
            break
    
    return video_id


def video_details(video_data):
    v_data=[]
    for v_id in video_data:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        
        )
        response = request.execute()

        for item in response['items']:
            data = {"channel_title":item['snippet']['channelTitle'],
                    "channel_id":item['snippet']['channelId'], 
                    "video_title":item['snippet']['title'],
                    "video_id":item['id'],
                    "video_description":item['snippet'].get('description'),
                    "duration":item['contentDetails']['duration'],
                    "caption":item['contentDetails']['caption'],
                    "view_count":item['statistics'].get('viewCount'),
                    "fav_count":item['statistics']['favoriteCount'],
                    "like_count":item['statistics'].get('likeCount'),
                    "comment_count":item['statistics'].get('commentCount')
                    }
            v_data.append(data)
    return v_data


def comment_details(video_data):
    c_data = []
    try:
        for v_id in video_data:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=v_id,
                maxResults=20
            
            )
            response = request.execute()

            for item in response['items']:
                data = {"Video_id":item['snippet']['topLevelComment']['snippet']['videoId'],
                        "Comment_id":item['snippet']['topLevelComment']['id'],
                        "Comment_text":item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_author":item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        }
                c_data.append(data)
    except:
        pass     
    return c_data       

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['Project_1']

def channel_details(channel_id):
    channel_datas=channel_ids(channel_id)
    video_info_ids=video_ids(channel_id)
    video_info_details=video_details(video_info_ids)
    comment_info_details=comment_details(video_info_ids)

    col1=db['channel_information']
    col1.insert_one({"channel_information":channel_datas,
                     "video_Idsinfo":video_info_ids,
                     "video_information":video_info_details,
                     "comment_information":comment_info_details})
    
    return "upload successfully completed"

def channel_table(channel_name_s):
    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()

    query = """create table if not exists channels(
    channel_name varchar(250),
    channel_publishedOn varchar(250),
    channel_id varchar(250) primary key,
    channel_description text,
    channel_subscriptionCount bigint,
    channel_viewCount int,
    channel_videoCount int )"""

    cursor.execute(query)
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]

    single_ch_details = []
    for data in col1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_ch_details.append(data['channel_information'])
    df_single_ch_details = pd.DataFrame(single_ch_details) 

    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()

    for index,row in df_single_ch_details.iterrows():
        query = """insert into channels(channel_name ,
                                    channel_publishedOn,
                                    channel_id,
                                    channel_description,
                                    channel_subscriptionCount,
                                    channel_viewCount ,
                                    channel_videoCount )   
                                    
                                    values(%s,%s,%s,%s,%s,%s,%s)"""


        values = (row['channel_name'],
                row['channel_publishedOn'],
                row['channel_id'],
                row['channel_description'],
                row['channel_subscriptionCount'],
                row['channel_viewCount'],
                row['channel_videoCount'])
        try:
        
            cursor.execute(query,values)
            client.commit()
        
        except:
        
            news= "Channel You have entered is already exist" 
            return news   

    cursor.close()
    client.close()
        
def videos_table(channel_name_s):
    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()

    query = """create table if not exists videos( channel_title varchar(250),
                                    channel_id varchar(100),
                                    video_title varchar(250),
                                    video_id varchar(100) primary key,
                                    video_description text,
                                    duration varchar(100),
                                    caption varchar(100),
                                    view_count bigint,
                                    fav_count int,
                                    like_count int,
                                    comment_count int
                                    )"""

    cursor.execute(query)

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]
    
    single_video_details = []
    for data in col1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_video_details.append(data['video_information'])
    df_single_video_details = pd.DataFrame(single_video_details[0])  

    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()   

    for index,row in df_single_video_details.iterrows():
        query ="""insert into videos(channel_title,
                                    channel_id,
                                    video_title,
                                    video_id,
                                    video_description,
                                    duration,
                                    caption,
                                    view_count,
                                    fav_count,
                                    like_count,
                                    comment_count 
                                    )   
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
 
        values = (row['channel_title'],
                row['channel_id'],
                row['video_title'],
                row['video_id'],
                row['video_description'],
                row['duration'],
                row['caption'],
                row['view_count'],
                row['fav_count'],
                row['like_count'],
                row['comment_count'] )
        
        cursor.execute(query,values)
        client.commit()   
    cursor.close()
    client.close()

    

def comments_table(channel_name_s):
    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()

    query = """create table if not exists comments(
                                    video_id varchar(100),
                                    Comment_id varchar(250) primary key,
                                    Comment_text text,
                                    Comment_author varchar(250)
                                    )"""

    cursor.execute(query)

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]
    
    single_comment_details = []
    for data in col1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_comment_details.append(data['comment_information'])
    df_single_comment_details = pd.DataFrame(single_comment_details[0])  

    client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
    )   
    cursor = client.cursor()

    for index,row in df_single_comment_details.iterrows():
        query = """insert into comments(
                                    Video_id,
                                    Comment_id,
                                    Comment_text,
                                    Comment_author )       
                                    
                                    values(%s,%s,%s,%s)"""
        

        values = (row['Video_id'],
                row['Comment_id'],
                row['Comment_text'],
                row['Comment_author'])

        cursor.execute(query,values)
        client.commit()
    cursor.close()
    client.close()
    
       
def tables(single_channel):
    news= channel_table(single_channel)
    if news:
        return news
    else:
        videos_table(single_channel)
        comments_table(single_channel)
        
        return "Tables created sucessfully"
    
    
def show_channels_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]

    ch_details = []
    for data in col1.find({},{"_id":0,"channel_information":1}):
        ch_details.append(data["channel_information"])
    df = st.dataframe(ch_details)

    return df

def show_videos_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]

    vi_details = []
    for vi_data in col1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
             vi_details.append(vi_data["video_information"][i])
    df1 = st.dataframe(vi_details)  

    return df1 

def show_comments_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]
    
    cm_details = []
    for c_data in col1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(c_data["comment_information"])):
            cm_details.append(c_data["comment_information"][i])
    df2 = st.dataframe(cm_details)
    
    return df2

st.title(":red[YOUTUBE DATA HARVESTING & WAREHOUSING]")


channel_id=st.text_input("Enter the channel ID")

if st.button("Collect & Store Datas"):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Project_1']
    col1=db["channel_information"]

    ch_ids  = []
    for data in col1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given Channel ID is already Exists")
    else:
        insert= channel_details(channel_id)
        st.success(insert)

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['Project_1']
col1=db["channel_information"]
all_channels=[]
for data in col1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(data['channel_information']['channel_name'])

unique_values=st.selectbox("Select the Channels",all_channels)

if st.button("Migrate to SQL"):
    Table=tables(unique_values)
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("Channels","Videos","Comments"))

if show_table=="Channels":
    show_channels_table()

elif show_table=="Videos":
    show_videos_table()

elif show_table=="Comments":
    show_comments_table()

client = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azarudeen1997",
    database="Youtube_Data"
)   
cursor = client.cursor()

question=st.selectbox("Select Your Questions",("1. What are the names of all the videos and their corresponding channels?",
                                               "2. Which channels have the most number of videos, and how many videos do they have?",
                                               "3. What are the top 10 most viewed videos and their respective channels?",
                                               "4. How many comments were made on each video, and what are their corresponding video names?",
                                               "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                               "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "9. Which videos have the highest number of comments, and what are their corresponding channel names?",
                                               "10 . Which channels have the most number of subscription and what are their corresponding channel names and published date"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1="""select video_title ,channel_title from videos"""
    cursor.execute(query1)
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=['Video Title','Channel title'])
    st.write(df)                                               

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2="""select channel_name , channel_videoCount from channels order by channel_videoCount DESC"""
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=['Channel Name','Total no.of.Videos'])
    st.write(df2)                                               

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3="""select view_count,channel_title,video_title from videos where view_count is not null order by view_count DESC limit 10 """
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Top 10 Views','Channel Name','Video Title'])
    st.write(df3)                                              

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4="""select comment_count , video_title from videos where comment_count is not null """
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['Total no.of.Comments','Video Title'])
    st.write(df4)                                               

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5="""select like_count , video_title , channel_title from videos where like_count is not null order by like_count DESC"""
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Highest no.of.Likes','Video Title','Channel Name'])
    st.write(df5)

elif question=="6. What is the total number of likes for each video, and what are their corresponding video names?":
    query6="""select like_count , video_title  from videos where like_count is not null"""
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Total no.of.Likes','Video Title'])
    st.write(df6)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7="""select channel_viewCount , channel_name  from channels where channel_viewCount is not null"""
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Total no.of.Views','Channel Name'])
    st.write(df7)

elif question=="8. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query8="""select duration,channel_title from videos """
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Average Duration','Channel Name'])
    st.write(df8)

elif question=="9. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query9="""select comment_count , video_title ,  channel_title  from videos where  comment_count is not null order by comment_count DESC"""
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Highest no.of.comments','Video Title','Channel Name'])
    st.write(df9)

elif question=="10 . Which channels have the most number of subscription and what are their corresponding channel names and published date":
    query10="""select channel_name , channel_subscriptionCount ,channel_publishedOn from channels where channel_subscriptionCount is not null order by channel_subscriptionCount DESC """
    cursor.execute(query10)
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Channel Name','Highest no.of.Subscription','Published Date'])
    st.write(df10)
