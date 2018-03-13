import os
import json
import shutil
import requests
from itertools import repeat
from multiprocessing import Pool
import pandas as pd

from config import *

def get_context(playlist_id):
    '''
    Makes directory for a playlist (channel)
    Returns the name of the metadata file.
    '''
    channel_dir = os.path.join(root_dir, playlist_id)
    metadata_filename = os.path.join(channel_dir, 'video_metadata.tsv')
    urls_filename = os.path.join(channel_dir, 'video_urls.csv')
    
    os.makedirs(channel_dir, exist_ok=True)
    shutil.chown(channel_dir, group='smapp')
    return metadata_filename, urls_filename

def is_user(channel_url):
    '''
    Checks if url is channel or user
    '''
    if 'youtube.com/user/' in channel_url:
        return True
    elif 'youtube.com/channel/' in channel_url:
        return False
    else:
        return

def get_youtube_id(channel_url):
    '''
    From a URL returns the YT ID.
    '''
    return channel_url.rstrip('/').split('/')[-1]


def get_playlist_id(username, key):
    '''
    Get a playlist ID (channel) from a username
    '''
    url = ("https://www.googleapis.com/youtube/v3/channels"
           "?part=contentDetails"
           "&forUsername={}&key={}".format(username,key))
    response = requests.get(url)
    if response.ok:
        response_json = json.loads(response.text)
        if "items" in response_json:
            if response_json['items']:
                channel_id = response_json['items'][0]['id']
                return channel_id
    return -1

def get_video_urls_from_playlist_id(playlist_id, key):
    '''
    Returns all video URLs from a play list id.
    '''
    url = ("https://www.googleapis.com/youtube/v3/playlistItems"
           "?part=snippet&playlistId={}"
           "&maxResults=50"
           "&key={}".format(playlist_id, key))
    
    next_page_token = None
    ids = []
    run = True
    while run:
        if next_page_token: 
            url += "&pageToken={}".format(next_page_token)
        response = requests.get(url)
        if response.ok:
            response_json = json.loads(response.text)
            for item in response_json['items']:
                ids.append(item['snippet']['resourceId']['videoId'])
            try: 
                next_page_token = response_json['nextPageToken']
            except:
                run = False
            print(">> {} Videos to parse".format(len(ids)))
        else:
            print(response)
            run = False
    
    return ids

def parse_video_metadata(item):
    '''
    Parses a JSON object for relevant fields
    '''
    video_meta = dict(
        channel_title = item["snippet"]["channelTitle"],
        channel_id =item["snippet"]["channelId"],
        video_publish_date = item["snippet"]["publishedAt"],
        video_title = item["snippet"]["title"],
        video_description = item["snippet"]["description"],
        video_category = item["snippet"]["categoryId"],
        video_view_count = item["statistics"]["viewCount"],
        video_comment_count = item["statistics"]["commentCount"],
        video_like_count = item["statistics"]["likeCount"],
        video_dislike_count = item["statistics"]["dislikeCount"],
        video_thumbnail = item["snippet"]["thumbnails"]["high"]["url"],
        collection_date = today
    )
    
    return video_meta

def get_video_metadata(video_id, key):
    '''
    Gets the raw video metadata, and parses it.
    '''
    http_endpoint = ("https://www.googleapis.com/youtube/v3/videos"
                     "?part=statistics,snippet"
                     "&id={}&key={}".format(video_id, key))
    response = requests.get(http_endpoint)
    if response.ok:
        response_json = json.loads(response.text)
        if 'items' in response_json:
            video_meta = response_json['items'][0]
            return parse_video_metadata(video_meta)
    return -1

def parse_channel(channel):
    print(channel)
    yt_id = get_youtube_id(channel)
    playlist_id = get_playlist_id(yt_id, key) if is_user(channel) else yt_id    
    if not playlist_id:
        print(">> Getting the playlist ID is not working for {}".format(yt_id))
        return
    # some quirk with channel or playlist names, go figure...
    playlist_id = 'UU' + playlist_id[2:]
    metadata_filename, urls_filename = get_context(playlist_id)
    if not os.path.exists(metadata_filename):
        if not os.path.exists(urls_filename):
            video_urls = get_video_urls_from_playlist_id(playlist_id, key)
            if not video_urls:
                print(">> Listing Video URLs is not working for {}".format(yt_id))
                return
            pd.DataFrame(video_urls).to_csv(urls_filename, index=False)
        else:
            video_urls = pd.read_csv(urls_filename)['0'].tolist()

        # parse each video from the user
        video_meta = []
        for video_url in video_urls:
            video_meta.append(get_video_metadata(video_url, key))
        df = pd.DataFrame(video_meta)
        df.to_csv(metadata_filename, index=False, sep='\t')  
        shutil.chown(metadata_filename, group='smapp')


def main():
    df_users = pd.read_csv(input_file)
    channels = df_users['users'].tolist()
    with Pool(4) as pool:
        pool.map(parse_channel, channels)

main()