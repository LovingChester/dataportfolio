import requests
import mysql.connector
import re
from datetime import date

'''
This Python Script extracts videos from the given YouTube
Channel and stores them into the database.
'''

# Delete it when upload to GitHub
API_KEY = ''
CHANNEL_ID = 'UCU2PacFf99vhb3hNiYDmxww'

#url = 'https://www.googleapis.com/youtube/v3/search'
# ?key='+API_KEY+'&channelId='+CHANNEL_ID+'&part=snippet,id&maxResults=50'

page = 0
next_page_token = None
unique = set()
data_videos = []
count = 0
while True:
    print(page+1)
    # Fetch items for the current page
    search_url = 'https://www.googleapis.com/youtube/v3/search'
    search_params = {
            "channelId": CHANNEL_ID,
            "part": "snippet,id",
            "maxResults": 50,
            "pageToken": next_page_token,
            "key": API_KEY
    }

    search_response = requests.get(search_url, search_params).json()
    #print(search_response['items'][0])
    for item in search_response['items']:
        #print(item)
        count += 1
        if item['snippet']['liveBroadcastContent'] == 'live' or \
            item['id']['kind'] != 'youtube#video':
            continue
        video_id = item['id']['videoId']
        recordTime = date.today()
        unique.add((video_id, recordTime))
        title = item['snippet']['title']
        pattern = r'[^a-zA-Z0-9]'
        title = re.sub(pattern, ' ', title)
        #title = title.replace('   ', ' ').replace('  ', ' ').replace(' 39 ', "'").replace(' quot ', '"')
        title = title.replace('  39 ', "'").replace(' quot ', '"').replace(' amp ', '&')
        title = title.replace('   ', ' ').replace('  ', ' ')
        title = title.strip().lower()

        publishedAt = item['snippet']['publishedAt'].split('T')[0]
        
        video_url = 'https://www.googleapis.com/youtube/v3/videos'
        video_params = {
            "id": video_id,
            "part": "contentDetails,statistics",
            "key": API_KEY
        }

        video_response = requests.get(video_url, video_params).json()
        
        # Parse the Duration, which is in form PT#H#M#S
        duration = video_response['items'][0]['contentDetails']['duration']
        duration = duration[2:]
        if duration.find('D') != -1:
            continue
        H_idx, M_idx, S_idx = duration.find('H'), duration.find('M'), duration.find('S')
        seconds = 0
        if H_idx != -1 and M_idx != -1 and S_idx != -1:
            seconds += int(duration[:H_idx])*3600 + int(duration[H_idx+1:M_idx])*60 + int(duration[M_idx+1:S_idx])
        elif H_idx == -1 and M_idx != -1 and S_idx != -1:
            seconds += int(duration[:M_idx])*60 + int(duration[M_idx+1:S_idx])
        elif H_idx != -1 and M_idx == -1 and S_idx != -1:
            seconds += int(duration[:H_idx])*3600 + int(duration[H_idx+1:S_idx])
        elif H_idx != -1 and M_idx != -1 and S_idx == -1:
            seconds += int(duration[:H_idx])*3600 + int(duration[H_idx+1:M_idx])*60
        elif H_idx != -1 and M_idx == -1 and S_idx == -1:
            seconds += int(duration[:H_idx])*3600
        elif H_idx == -1 and M_idx != -1 and S_idx == -1:
            seconds += int(duration[:M_idx])*60
        elif H_idx == -1 and M_idx == -1 and S_idx != -1:
            seconds += int(duration[:S_idx])

        duration = seconds
        viewCount = int(video_response['items'][0]['statistics']['viewCount'])
        likeCount = int(video_response['items'][0]['statistics']['likeCount'])
        favoriteCount = int(video_response['items'][0]['statistics']['favoriteCount'])
        commentCount = int(video_response['items'][0]['statistics']['commentCount'])

        data_video = (video_id, recordTime, title, publishedAt, duration, viewCount, likeCount, favoriteCount, commentCount)
        data_videos.append(data_video)
        
    # Check if there are more pages to fetch
    next_page_token = search_response.get('nextPageToken')
    if not next_page_token:
        break

    page += 1

print(len(data_videos))
print(count)
print(len(unique))

#### DATABASE ####
'''
Video(videoId, recordTime, title, publishedAt, duration, viewCount, likeCount, favoriteCount, commentCount)
Keys: videoId, (title, publishAt)
Primary Attributes: videoId, title, publishAt
Functional Dependencies:
videoId, recordTime->title, publishedAt, duration, viewCount, likeCount, favoriteCount, commentCount
title, publishedAt->duration, viewCount, likeCount, favoriteCount, commentCount
'''

try:
    cnx = mysql.connector.connect(user='root', password='Superboy2008@#$',
                              host='localhost',
                              database='chelseachannel')
except mysql.connector.Error as err:
    print(err)


cursor = cnx.cursor()

# Create the videos table
create_table = ("CREATE TABLE IF NOT EXISTS videos ("
                "videoid VARCHAR(15) NOT NULL,"
                "recordTime DATE NOT NULL,"
                "title VARCHAR(200) DEFAULT NULL,"
                "publishedAt DATE DEFAULT NULL,"
                "duration INT(11) DEFAULT NULL,"
                "viewCount INT(11) DEFAULT NULL,"
                "likeCount INT(11) DEFAULT NULL,"
                "favoriteCount INT(11) DEFAULT NULL,"
                "commentCount INT(11) DEFAULT NULL,"
                "PRIMARY KEY (videoid, recordTime)"
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8"
                )

cursor.execute(create_table)

add_video = ("INSERT INTO videos "
            "(videoid, recordTime, title, publishedAt, duration, viewCount, likeCount, favoriteCount, commentCount) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "title = VALUES(title),"
            "viewCount = VALUES(viewCount),"
            "likeCount = VALUES(likeCount),"
            "favoriteCount = VALUES(favoriteCount),"
            "commentCount = VALUES(commentCount)")

# Insert the data
for data_video in data_videos:
    #print(data_video)
    cursor.execute(add_video, data_video)


cnx.commit()
cursor.close()
cnx.close()
