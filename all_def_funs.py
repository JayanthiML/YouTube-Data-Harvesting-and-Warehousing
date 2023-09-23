import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.errors import HttpError
import time
import pymongo
import pandas as pd
from datetime import datetime
import mysql.connector



## EXTRACT

def Youtube_API(channel_ids, api_key):

    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    def channel_details(channel_ids):
        channel_data = {}

        for channel_id in channel_ids:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
            )
            response = request.execute()

            if 'items' in response and len(response['items']) > 0:
                channel_info = response['items'][0]['snippet']
                channel_statistics = response['items'][0]['statistics']
                channels_playlist_id = response['items'][0]['contentDetails']

                channel_details = {
                    'Channel_Name': {
                        'Channel_Name': channel_info['title'],
                        'Channel_Id': channel_id,
                        'Subscription_Count': int(channel_statistics.get('subscriberCount', 0)),
                        'Channel_Views': int(channel_statistics.get('viewCount', 0)),
                        'Channel_Description': channel_info.get('description', 'No Data') or 'No Data',
                        'Playlist_Id': channels_playlist_id['relatedPlaylists']['uploads']
                    }
                }

                channel_data[channel_id] = channel_details

        return channel_data
    import re

    def format_duration(duration_str):
        match = re.match(r'PT(\d+)M(\d+)S', duration_str)
        if match:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            return f"00:{minutes:02d}:{seconds:02d}"
        else:
            return "00:00:00"
        
    def video_details(playlist_id):

        video_data = []
        
        next_page_token = None

        retry_attempts = 3
        while retry_attempts > 0:
            try:
                request = youtube.playlistItems().list(
                    part="snippet",
                    maxResults=50,
                    playlistId=playlist_id,
                    pageToken=next_page_token
                )

                response = request.execute()

                for item in response.get("items", []):
                    video_info = item.get("snippet", {})
                    video_id = video_info.get("resourceId", {}).get("videoId", "No Data")
                    video_name = video_info.get("title", "No Data")
                    video_description = video_info.get("description", "No Data") or "No Data"
                    published_date = video_info.get("publishedAt", 0)
                    # Fetch video statistics
                    video_statistics = youtube.videos().list(
                        part="statistics",
                        id=video_id
                    ).execute()
                    # Extract statistics if available
                    if 'items' in video_statistics:
                        statistics = video_statistics['items'][0]['statistics']
                        view_count = statistics.get('viewCount', 0)
                        like_count = statistics.get('likeCount', 0)
                        dislike_count = statistics.get('dislikeCount', 0)
                        favorite_count = statistics.get('favoriteCount', 0)
                        comment_count = statistics.get('commentCount', 0)
                    else:
                        view_count = like_count = dislike_count = favorite_count = comment_count = 0

                    # Fetch video duration and format it
                    video_content_details = youtube.videos().list(
                        part="contentDetails",
                        id=video_id
                    ).execute()

                    # Extract duration if available
                    if 'items' in video_content_details:
                        duration = format_duration(video_content_details['items'][0]['contentDetails']['duration'])
                    else:
                        duration = "No Data"

                    # Fetch video thumbnail
                    video_thumbnails = video_info['thumbnails']
                    thumbnail = video_thumbnails.get('default', {}).get('url', 'NO DATA')

                    # Fetch caption status if available
                    caption_status = video_info.get('defaultLanguage', 'NOT AVAILABLE')

                    video_data.append({
                        "Video_Id": video_id,
                        "Video_Name": video_name,
                        "Video_Description": video_description,
                        "PublishedAt": published_date,
                        "View_Count": view_count,
                        "Like_Count": like_count,
                        "Dislike_Count": dislike_count,
                        "Favorite_Count": favorite_count,
                        "Comment_Count": comment_count,
                        "Duration": duration,
                        "Thumbnail": thumbnail,
                        "Caption_Status": caption_status,
                        "Comments": []
                    })


                next_page_token = response.get("nextPageToken")

                if not next_page_token:
                    break

            except HttpError as e:
                if e.resp.status == 403 and "rateLimitExceeded" in str(e):
                    print(f"Rate limit exceeded. Retrying in 60 seconds...")
                    time.sleep(60)  
                    retry_attempts -= 1
                else:
                    print(f"An error occurred: {e}")
                    break  

        return video_data

    def extract_comment_details(video_id):

        comment_details = []

        try:
            # Fetch video statistics
            request_stats = youtube.videos().list(
                part="statistics",
                id=video_id
            )
            response_stats = request_stats.execute()
            video_statistics = response_stats['items'][0]['statistics'] if 'items' in response_stats and len(
                response_stats['items']) > 0 else {}

            comment_count = int(video_statistics.get("commentCount", "0"))

            if comment_count > 0:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    textFormat="plainText",
                    maxResults=30
                )

                response = request.execute()

                if 'items' in response:
                    for comment in response['items']:
                        comment_data = {
                            'Comment_Id': comment['id'],
                            'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'Comment_PublishedAt': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        comment_details.append(comment_data)

        except HttpError as e:
            print(f"An error occurred: {e}")

        return comment_details

    def comment_details(playlist_id):

        video_data = video_details(playlist_id)

        for video in video_data:
            video['Comments'] = extract_comment_details(video['Video_Id'])

        return video_data

    channel_data = channel_details(channel_ids)
    output_data = {}

    for channel_id, channel_info in channel_data.items():
        playlist_id = channel_info['Channel_Name']['Playlist_Id']
        videos_data = comment_details(playlist_id)
        channel_info['Videos'] = videos_data
        output_data[channel_info['Channel_Name']['Channel_Name']] = channel_info

    return output_data


## TO MONGODB

def insert_data_to_mongodb(channel_ids, api_key):
    connection_string = "mongodb+srv://jayanthi:MongoDBPassword@guviproject.o4vl81m.mongodb.net/?retryWrites=true&w=majority"
    mongodb_client = pymongo.MongoClient(connection_string)
    mongodb_db = mongodb_client["Youtube_API"]
    mongodb_collection = mongodb_db["Youtube_Channel_Details"]

    rawdata = Youtube_API(channel_ids, api_key)

    for channel_id, data in rawdata.items():
        mongodb_collection.insert_one(data)
    
    mongodb_client.close()


## FROM MONGODB AS JSON

def get_mongodb_data_as_json():
    connection_string = "mongodb+srv://jayanthi:MongoDBPassword@guviproject.o4vl81m.mongodb.net/?retryWrites=true&w=majority"
    mongodb_client = pymongo.MongoClient(connection_string)
    mongodb_db = mongodb_client["Youtube_API"]
    mongodb_collection = mongodb_db["Youtube_Channel_Details"]

    documents = mongodb_collection.find({})
    json_data = []

    for doc in documents:
        doc["_id"] = str(doc["_id"])
        json_data.append(dict(doc))

    mongodb_client.close()

    return json_data



# To DF

def channel_dataframe():

    json_data = get_mongodb_data_as_json()
    
    Channel = []

    for entry in json_data:
        channel_entry = {
            "Channel ID": entry["Channel_Name"]["Channel_Id"],
            "Channel Name": entry["Channel_Name"]["Channel_Name"],
            "Channel Views": entry["Channel_Name"]["Channel_Views"],
            "Channel Description": entry["Channel_Name"]["Channel_Description"],
            "Subscription Count": entry["Channel_Name"]["Subscription_Count"],
            "Playlist ID": entry["Channel_Name"]["Playlist_Id"]
        }
        Channel.append(channel_entry)

    df_Channel = pd.DataFrame(Channel)
    return df_Channel

def videos_dataframe():

    json_data = get_mongodb_data_as_json()

    Videos = []

    for entry in json_data:
        for video in entry.get("Videos", []):
            published_date_str = video.get("PublishedAt", None)
            if published_date_str:
                try:
                    published_date = datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ")
                    formatted_date = published_date.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    print(f"Error parsing date: {e}")
                    formatted_date = None
            else:
                formatted_date = None
            video_entry = {
                "Video ID": video.get("Video_Id", None),
                "Video Name": video.get("Video_Name", None),
                "Video Description": video.get("Video_Description", None),
                "Published Date":formatted_date,
                "View Count": video.get("View_Count", None),
                "Like Count": video.get("Like_Count", None),
                "Dislike Count": video.get("Dislike_Count", None),
                "Favourite Count": video.get("Favorite_Count", None),
                "Comment Count": video.get("Comment_Count", None),
                "Duration": video.get("Duration", None),
                "Thumbnail": video.get("Thumbnail", None),
                "Caption Status": video.get("Caption_Status", None),
                "Channel ID": entry["Channel_Name"]["Channel_Id"]
            }
            Videos.append(video_entry)  

    df_Videos = pd.DataFrame(Videos)
    return df_Videos


def comments_dataframe():

    json_data = get_mongodb_data_as_json()

    Comments = []

    for entry in json_data:
        for video in entry.get("Videos", []):
            for comment in video.get("Comments", []):
                comment_entry = {
                    "Video ID": video.get("Video_Id", None),
                    "Comment ID": comment.get("Comment_Id", None),
                    "Comment Text": comment.get("Comment_Text", None),
                    "Comment Author": comment.get("Comment_Author", None),
                    "Comment Published Date": datetime.strptime(comment.get("Comment_PublishedAt", None), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S"),
                    "Channel ID": entry["Channel_Name"]["Channel_Id"]
                }
                Comments.append(comment_entry)

    df_comments = pd.DataFrame(Comments)
    return df_comments


# TO MYSQL

def migrate_channel_to_mysql():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_views INT,
        channel_description TEXT,
        subscription_count INT,
        playlist_id VARCHAR(255)
    )
    """)

    insert_query = """
    INSERT INTO channel (channel_id, channel_name, channel_views, channel_description, subscription_count, playlist_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    Channel_df = channel_dataframe()

    for _, row in Channel_df.iterrows():
        values = tuple(row)
        mysql_cursor.execute(insert_query, values)

    mysql_connection.commit()


def migrate_videos_to_mysql():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS videos (
        `Video_ID` VARCHAR(255) PRIMARY KEY,
        `Video_Name` VARCHAR(255),
        `Video_Description` TEXT,
        `Published_Date` DATETIME,
        `View_Count` INT,
        `Like_Count` INT,
        `Dislike_Count` INT,
        `Favorite_Count` INT,
        `Comment_Count` INT,
        `Duration` TIME,
        `Thumbnail` VARCHAR(255),
        `Caption_Status` VARCHAR(255),
        `Channel_ID` VARCHAR(255),
        FOREIGN KEY (`Channel_ID`) REFERENCES `channel`(`Channel_ID`)
    );
    """)

    insert_query = """
    INSERT INTO videos (
        `Video_ID`, `Video_Name`, `Video_Description`, `Published_Date`,
        `View_Count`, `Like_Count`, `Dislike_Count`, `Favorite_Count`,
        `Comment_Count`, `Duration`, `Thumbnail`, `Caption_Status`, `Channel_ID`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    Video_df = videos_dataframe()

    for _, row in Video_df.iterrows():
        values = tuple(row)
        mysql_cursor.execute(insert_query, values)

    mysql_connection.commit()


def migrate_comments_to_mysql():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""CREATE TABLE comments (
    comment_id VARCHAR(255) PRIMARY KEY,
    Video_ID VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_date DATETIME,
    channel_id VARCHAR(255)
    );
    """)

    insert_query = """
    INSERT INTO comments (Video_ID, comment_id, comment_text, comment_author, comment_published_date, channel_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    comments_df = comments_dataframe()

    for _, row in comments_df.iterrows():
        values = tuple(row)
        mysql_cursor.execute(insert_query, values)

    mysql_connection.commit()


# QUESTIONS

def Question1():

    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()    
    
    mysql_cursor.execute("""SELECT c.channel_name, v.Video_Name FROM videos v INNER JOIN channel c ON v.Channel_ID = c.channel_id
""")
    
    results = mysql_cursor.fetchall()

    df1 = pd.DataFrame(results, columns=["Channel Name", "Video Name"])

    return df1


def Question2():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT c.channel_name, COUNT(v.Video_ID) AS num_videos
FROM channel c
INNER JOIN videos v ON c.channel_id = v.Channel_ID
GROUP BY c.channel_name
ORDER BY num_videos DESC;
""")
    
    results = mysql_cursor.fetchall()

    df2 = pd.DataFrame(results, columns=["Channel Name", "Number of Videos"])

    return df2


def Question3():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT v.Video_Name, c.channel_name, v.View_Count
FROM videos v
INNER JOIN channel c ON v.Channel_ID = c.channel_id
ORDER BY v.View_Count DESC
LIMIT 10;
""")
    
    results = mysql_cursor.fetchall()

    df3 = pd.DataFrame(results, columns=["Video Name", "Channel Name", "View Count"])

    return df3


def Question4():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT v.Video_Name, v.comment_count AS Number_of_Comments
FROM videos v;
""")
    
    results = mysql_cursor.fetchall()

    df4 = pd.DataFrame(results, columns=["Video Name", "Number of Comments"])

    return df4

def Question5():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT v.Video_Name, c.channel_name, v.Like_Count
FROM videos v
INNER JOIN channel c ON v.Channel_ID = c.channel_id
ORDER BY v.Like_Count DESC
LIMIT 10;  -- You can adjust the limit as needed to get the top N videos
""")
    
    results = mysql_cursor.fetchall()

    df5 = pd.DataFrame(results, columns=["Video Name", "Channel Name", "Like Count"])

    return df5


def Question6():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT v.Video_Name, SUM(v.Like_Count) AS Total_Likes, SUM(v.Dislike_Count) AS Total_Dislikes
FROM videos v
GROUP BY v.Video_Name;
""")
    
    results = mysql_cursor.fetchall()

    df6 = pd.DataFrame(results, columns=["Video Name", "Total Likes", "Total Dislikes"])

    return df6


def Question7():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT c.channel_name, SUM(v.View_Count) AS Total_Views
FROM channel c
LEFT JOIN videos v ON c.channel_id = v.Channel_ID
GROUP BY c.channel_name;
""")
    
    results = mysql_cursor.fetchall()

    df7 = pd.DataFrame(results, columns=["Channel Name", "Total Views"])

    return df7


def Question8():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT c.channel_name, v.Video_Name
FROM channel c
INNER JOIN videos v ON c.channel_id = v.Channel_ID
WHERE YEAR(v.Published_Date) = 2022;
""")
    
    results = mysql_cursor.fetchall()

    df8 = pd.DataFrame(results, columns=["Channel Name", "Video Name"])

    return df8


def Question9():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()

    mysql_cursor.execute("""
SELECT c.channel_name, CONCAT(ROUND(AVG(v.Duration), 2), ' seconds') AS 'Average Duration'
FROM channel c
LEFT JOIN videos v ON c.channel_id = v.Channel_ID
GROUP BY c.channel_name;
""")
    
    results = mysql_cursor.fetchall()

    df9 = pd.DataFrame(results, columns=["Channel Name", "Average Duration"])

    return df9


def Question10():
    
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="MySql_Password",
        db="Capstone_Project_SQL",
    )

    mysql_cursor = mysql_connection.cursor()
    
    mysql_cursor.execute("""
SELECT v.Video_Name, c.channel_name, v.Comment_Count AS Comment_Count
FROM videos v
INNER JOIN channel c ON v.Channel_ID = c.channel_id
ORDER BY v.Comment_Count DESC
LIMIT 5;
""")
    
    results = mysql_cursor.fetchall()

    df10 = pd.DataFrame(results, columns=["Video Name", "Channel Name", "Comment Count"])

    return df10

    mysql_cursor.close()
    mysql_connection.close()



# FOR INDIVIDUAL PLAYLIST

def playlist_details(channel_ids, api_key):

    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    playlist_data = []  

    for channel_id in channel_ids:
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=25
        )
        response = request.execute()

        for playlist_item in response.get("items", []):
            playlist_id = playlist_item["id"]
            playlist_name = playlist_item["snippet"]["title"]
            channelTitle = playlist_item["snippet"]["channelTitle"]


            playlist_data.append({
                "Playlist ID": playlist_id,
                "Playlist Name": playlist_name,
                "Channel ID": channel_id,
                "Channel Name": channelTitle,                
            })
    return playlist_data

def insert_playlist_to_mongodb(channel_ids, api_key):

    connection_string = "mongodb+srv://jayanthi:MongoDBPassword@guviproject.o4vl81m.mongodb.net/?retryWrites=true&w=majority"
    mdb_client = pymongo.MongoClient(connection_string)
    mdb_db = mdb_client["Youtube_API"]
    mdb_collection = mdb_db["Youtube_Playlist_Details"]

    playlistdata = playlist_details(channel_ids, api_key)

    for data in playlistdata:
        mdb_collection.insert_one(data)

def get_mongodb_playlistdata_as_json():

    connection_string = "mongodb+srv://jayanthi:MongoDBPassword@guviproject.o4vl81m.mongodb.net/?retryWrites=true&w=majority"
    mdb_client = pymongo.MongoClient(connection_string)
    mdb_db = mdb_client["Youtube_API"]
    mdb_collection = mdb_db["Youtube_Playlist_Details"]

    documents = mdb_collection.find({})
    json_playlist_data = []

    for doc in documents:
        doc["_id"] = str(doc["_id"])
        json_playlist_data.append(dict(doc))

    return json_playlist_data

def playlist_dataframe():

    playlist_json_data = get_mongodb_playlistdata_as_json()  
    
    Playlist = []

    for entry in playlist_json_data:
        playlist_entry = {
            "Playlist ID": entry["Playlist ID"],
            "Playlist Name": entry["Playlist Name"],
            "Channel ID": entry["Channel ID"],
            "Channel Name": entry["Channel Name"]
        }
        Playlist.append(playlist_entry)

    df_Playlist = pd.DataFrame(Playlist)
    return df_Playlist

def migrate_playlist_to_mysql():
    
    mysql_connection = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="MySql_Password",
            db="Capstone_Project_SQL",
        )

    mysql_cursor = mysql_connection.cursor()


    mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlists (
        playlist_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255),
        channel_name VARCHAR(255),
        FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
    )
    """)


    insert_query = """
    INSERT INTO playlists (playlist_id, playlist_name, channel_id, channel_name)
    VALUES (%s, %s, %s, %s)
    """


    playlist_df = playlist_dataframe()

    for _, row in playlist_df.iterrows():
        values = tuple(row)
        mysql_cursor.execute(insert_query, values)


    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

