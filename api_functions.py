import pandas as pd
from googleapiclient.discovery import build


def get_channels_stats(youtube, channel_ids):
    '''Function:
    Get the statistics data from all channels in the channel ids list
    
    Params:
    youtube: build object from googleapiclient.discovery
    channel_ids: list of ids from different channels on Youtube
    
    Return:
    A dataframe with all the statistics from the channel id list
    '''
    all_data = []
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids) #using join because we have multiple channels and we want to concatenate them.
    )
    response = request.execute()
    #loop through itens
    for item in response['items']:
        data = {'channel_name': item['snippet']['title'],
                'subscribers' : item['statistics']['subscriberCount'],
                'views':item['statistics']['viewCount'],
                'total_videos': item['statistics']['videoCount'],
                'playlist_Id': item['contentDetails']['relatedPlaylists']['uploads']
        }
        
        all_data.append(data)
        
    return(pd.DataFrame(all_data))

def get_video_ids(youtube, playlist_id):
    """ Function:
    Get list of video IDs from all videos in the playlist that was given
    
    Params:
    youtube: build object from googleapiclient.discovery
    playlist_id: playlist ID of the channel
    
    Returns:
    List of video IDs of all videos in the playlist
    
    """
    
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_comments_in_videos (youtube,video_ids):
    ''' Function:
    Get the first 10 comments from the youtube videos as text with given ID's
    The amount of comments it's due to the quotes from Youtube API
    
    Params:
    youtube: build object from from googleapiclient.discovery
    videos_ids: list of all videos Ids
    
    Return:
    A dataframe with the top 10 comments
    '''
    all_comments = []
    
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part = 'snippet, replies',
                videoId = video_id
            )
            response = request.execute()

            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_Id':video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)
        except:
            print('No comments in the video: '+video_id)
    return (pd.DataFrame(all_comments))

def get_videos_details (youtube,videos_ids):
    '''Function:
    Retrieve the informations about the videos from eache channel like published day, view count, duration of the video, etc.
    
    Params:
    youtube: build object from from googleapiclient.discovery
    videos_ids: list of all videos Ids
    
    '''
    all_video_info = []

    for i in range (0, len(videos_ids),50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id =','.join(videos_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title','description','tags','publishedAt'],
                             'statistics': ['viewCount','likeCount','favoriteCount', 'commentCount'],
                             'contentDetails': ['duration','definition','caption']
            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v]= video[k][v]
                    except:
                        video_info[v]= None

            all_video_info.append(video_info)
        
    return (pd.DataFrame(all_video_info))