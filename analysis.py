#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from joblib import Parallel, delayed
import gc
import sqlalchemy


API_KEY = "AIzaSyBbKU3w-HfuMVlrGGRLFNWZgCv0l8oaPq8"
#API_KEY = "<YOUR_API_KEY>"

MAX_VIDEOS = 300
DL_VIDEOS = 50

MySQL_URI = 'mysql+pymysql://username:password@host:port/database'
KEYWORD='python'



def get_utube_vids(keyword):
    """Get video based on keyword search"""
    global pageToken
    URL = "https://www.googleapis.com/youtube/v3/search"

    params = {
        'q':keyword,
        'order':'viewCount',
        'part':'id',
        'pageToken': pageToken,
        'maxResults': MAX_VIDEOS,
        'videoCategoryId':28, #Programming - Science & Technology
        'type':'video',
        'key': API_KEY
    }

    valA = requests.get(URL, params=params)
#     if not valA.ok:
#         return
    
    data = valA.json()
    try:
        pageToken = data['nextPageToken']
    except KeyError:
        pass
    return [e['id']['videoId'] for e in data['items']]


# In[36]:



# ## Get details video

# In[120]:



def get_vid_details(video_id):
    """Get Video Metadata"""
    
    URL = "https://youtube.googleapis.com/youtube/v3/videos"
    params = {
        'id':video_id,
        'part':'snippet,contentDetails,statistics',
        'key': API_KEY
    }

    valB = requests.get(URL, params=params)
    
    if not valB.ok:
        return
    data = valB.json()['items'][0]
    return {
        'vid':video_id,
        'tags':data['snippet'].get('tags', []),
        'video_title':data['snippet']['title'],
        'duration':data['contentDetails']['duration'],
        'statistics':data['statistics'],
        'channel_title':data['snippet']['channelTitle']

    }
    



def get_seconds(time_str):
    """Split PT12H23M23S to second integer"""
    try:
        hh =  time_str.split('H') if 'H' in time_str else (0, time_str, 0)
        h, m, s = hh
    except ValueError:
        return int(hh[0]) * 3600
    m, s = m.split('M') if 'M' in m else (0, m)
    s = s.split('S')[0]
    try:
        return int(h) * 3600 + int(m) * 60 + int(s)
    except ValueError:
        return int(m) * 60

    
def save_to_db(df, tags):
    con = sqlalchemy.create_engine(MySQL_URI)
    df.to_sql('videos', con=con, if_exists='replace', index=False)
    tags.to_sql('tags', con=con, if_exists='replace')



def main():
    pageToken = ''
    ids = []
    
    for i in range(int(MAX_VIDEOS/DL_VIDEOS) + 1):
        ids.append(get_utube_vids(KEYWORD))

    # Use all available threads to pull the data
    data = []
    for vids in ids:
        data_vids = Parallel(n_jobs=-1)(delayed(get_vid_details)(vid) for vid in vids)
        data.append(pd.DataFrame(data_vids))

    df = pd.concat(data,ignore_index=True)
    
    # We need to flatten statistics in JSON format to columns
    stats = pd.json_normalize(df['statistics']).fillna('0').astype(int)

    df = pd.concat([df,stats], axis='columns')

    del data
    del ids
    del df['statistics']
    gc.collect()

    
    df['duration_s'] = df.duration.str[2:].apply(get_seconds)

    tags = (df
            .explode('tags')
            .groupby('tags')
            .agg({
                'vid': 'nunique',
                'duration_s': 'mean',
                'viewCount': 'sum',
                'dislikeCount': 'sum',
                'likeCount': 'sum',
                'favoriteCount': 'sum',
                'commentCount':'sum'
                  }) 
           )
    
    #save_to_db(df, tags)
    tags.to_excel('data.xlsx')

    

if __name__ == "__main__":
    main()