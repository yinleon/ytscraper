import os
import time
import sys
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
    # output files
    metadata_filename = os.path.join(channel_dir, 'video_metadata.tsv')
    urls_filename = os.path.join(channel_dir, 'video_urls.csv')
    
    if IS_DEV:
        channel_dir = channel_dir + '__test'
        metadata_filename = metadata_filename.replace('.tsv', '__test.tsv')
        urls_filename = urls_filename.replace('.csv', '__test.csv')
    
    # make directories for outputs
    os.makedirs(channel_dir, exist_ok=True)
    shutil.chown(channel_dir, group='smapp')
    
    return metadata_filename, urls_filename

def log(text):
    '''
    A logger will be put here
    '''
    print(text)

def load_response(response): 
    '''
    Loads the response to json, and checks for errors.
    '''
    response_json = response.json()
    try: 
        response.raise_for_status()
    except: 
        response_json = handle_error(response_json)

    return response_json


def handle_error(error):
    '''
    Parses errors if the request raised a status.
    '''
    reasons = []
    for e in error['error']['errors']:
        reasons.append(e['reason'])
    if 'dailyLimitExceeded' in reasons:
        log(error)
        sys.exit()
    elif 'limitExceeded' in reasons:
        log(error)
        time.sleep(60 * 60)
    elif 'quotaExceeded' in reasons:
        log(error)
        time.sleep(60 * 60)
    elif 'badRequest' in reasons:
        log(error)
    return False
    
    
def is_user(channel_url):
    '''
    Checks if url is channel or user
    '''
    if 'youtube.com/user/' in channel_url:
        return True
    elif 'youtube.com/channel/' in channel_url:
        return False
    else:
        print("Didn't recognize url {}".format(channel_url))
        sys.exit()
        
        
def get_youtube_id(channel_url):
    '''
    From a URL returns the YT ID.
    '''
    return channel_url.rstrip('/').split('/')[-1]


def get_playlist_id(username, key):
    '''
    Get a playlist ID (channel) from a username.
    '''
    http_endpoint = ("https://www.googleapis.com/youtube/v3/channels"
                     "?part=contentDetails"
                     "&forUsername={}&key={}".format(username,key))
    response = requests.get(http_endpoint)
    response_json = load_response(response)
    if response_json:
        if "items" in response_json and response_json['items']:
            channel_id = response_json['items'][0]['id']
            return channel_id
    else:
        return response_json

def get_video_urls_from_playlist_id(playlist_id, key):
    '''
    Returns all video URLs from a play list id.
    '''
    http_endpoint = ("https://www.googleapis.com/youtube/v3/playlistItems"
                     "?part=snippet&playlistId={}"
                     "&maxResults=50&key={}".format(playlist_id, key))
    next_page_token = None
    video_ids = []
    iterations = 0
    run = True
    while run:
        if next_page_token: 
            http_endpoint += "&pageToken={}".format(next_page_token)    
        response = requests.get(http_endpoint)
        response_json = load_response(response)
        if response_json:
            for item in response_json['items']:
                video_ids.append(item['snippet']['resourceId']['videoId'])
            try: 
                next_page_token = response_json['nextPageToken']
                iterations += 1
            except:
                run = False
            if IS_DEV:
                if iterations > 2: 
                    run = False
            log(">> {} Videos to parse".format(len(video_ids)))
        time.sleep(1)
    
    return video_ids

def parse_video_metadata(item):
    '''
    Parses a JSON object for relevant fields
    '''    
    video_meta = dict(
        channel_title = item["snippet"].get("channelTitle"),
        channel_id =item["snippet"].get("channelId"),
        video_publish_date = item["snippet"].get("publishedAt"),
        video_title = item["snippet"].get("title"),
        video_description = item["snippet"].get("description"),
        video_category = item["snippet"].get("categoryId"),
        video_view_count = item["statistics"].get("viewCount"),
        video_comment_count = item["statistics"].get("commentCount"),
        video_like_count = item["statistics"].get("likeCount"),
        video_dislike_count = item["statistics"].get("dislikeCount"),
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
    response_json = load_response(response)
    video_meta = {}
    if response_json:   
        video_meta = response_json['items'][0]
        video_meta = parse_video_metadata(video_meta)
        
    return video_meta


def parse_channel(channel):
    log(channel)
    yt_id = get_youtube_id(channel)
    playlist_id = get_playlist_id(yt_id, key) if is_user(channel) else yt_id    
    if not playlist_id:
        log(">> Getting the playlist ID is not working for {}".format(yt_id))
        return
    # some quirk with channel or playlist names, go figure...
    playlist_id = 'UU' + playlist_id[2:]
    metadata_filename, urls_filename = get_context(playlist_id)
    if not os.path.exists(metadata_filename):
        if not os.path.exists(urls_filename):
            video_urls = get_video_urls_from_playlist_id(playlist_id, key)
            if not video_urls:
                log(">> Listing Video URLs is not working for {}".format(yt_id))
                return
            pd.DataFrame(video_urls).to_csv(urls_filename, index=False)
            shutil.chown(urls_filename, group='smapp')
            log(">>> Video urls to parse saved here: {}".format(urls_filename))

        else:
            log(">> Video urls saved previously.")
            video_urls = pd.read_csv(urls_filename)['0'].tolist()
        
        # parse each video from the user
        if IS_DEV: video_urls = video_urls[:100] 
        videos_meta = []
        for videos_url in tqdm(video_urls):
            v_m = get_video_metadata(video_url, key)
            videos_meta.append(v_m)
        df = pd.DataFrame(videos_meta)
        df.to_csv(metadata_filename, index=False, sep='\t')  
        shutil.chown(metadata_filename, group='smapp')
        log("!!!! Video metadata saved here: {}".format(metadata_filename))
    else:
        log("!!!! ABD")


def main():
    df_users = pd.read_csv(input_file)
    channels = df_users['users'].tolist()
    if IS_DEV: channels = channels[:10]
    for channel in channels:
        parse_channel(channel)
        time.sleep(3)

main()
