import os
from pathlib import Path
import csv
import json
from datetime import datetime
import time
from typing import Dict, Set, List
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle
import pocket_secrets

class YouTubeMetadataFetcher:
    def __init__(self, api_key: str):
        """
        Initialize the YouTube API client.
        
        Args:
            api_key (str): Your YouTube Data API key
        """
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.cache_file = os.path.join(str(Path.home()), 'youtube_metadata_cache.pkl')
        self.metadata_cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Load the metadata cache from disk."""
        try:
            with open(self.cache_file, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return {}
            
    def _save_cache(self):
        """Save the metadata cache to disk."""
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.metadata_cache, f)
        
    def _get_video_metadata(self, video_ids: List[str]) -> List[Dict]:
        """
        Fetch metadata for a batch of videos.
        
        Args:
            video_ids (List[str]): List of video IDs to fetch
            
        Returns:
            List[Dict]: List of video metadata dictionaries
        """
        results = []
        
        # YouTube API allows up to 50 videos per request
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            try:
                response = self.youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(batch)
                ).execute()
                
                for item in response.get('items', []):
                    metadata = {
                        'videoId': item['id'],
                        'duration': item['contentDetails']['duration'],
                        'publishedAt': item['snippet']['publishedAt'],
                        'title': item['snippet']['title'],
                        'channelId': item['snippet']['channelId'],
                        'channelTitle': item['snippet']['channelTitle'],
                        'categoryId': item['snippet'].get('categoryId', ''),
                        'definition': item['contentDetails']['definition'],
                        'viewCount': item['statistics'].get('viewCount', '0'),
                        'likeCount': item['statistics'].get('likeCount', '0'),
                        'commentCount': item['statistics'].get('commentCount', '0'),
                        'favoriteCount': item['statistics'].get('favoriteCount', '0')
                    }
                    results.append(metadata)
                    self.metadata_cache[item['id']] = metadata
                
                # Sleep to respect API quota
                time.sleep(0.1)
                
            except HttpError as e:
                print(f"Error fetching metadata for batch: {str(e)}")
                continue
                
        return results

    def process_history(self, input_csv: str, output_csv: str):
        """
        Process the watch history CSV and fetch metadata for new videos.
        
        Args:
            input_csv (str): Path to the input watch history CSV
            output_csv (str): Path to output the metadata CSV
        """
        # Read existing metadata if output file exists
        existing_metadata = set()
        if os.path.exists(output_csv):
            df = pd.read_csv(output_csv)
            existing_metadata = set(df['videoId'].unique())
        
        # Read watch history
        df = pd.read_csv(input_csv)
        
        video_ids = set(df['videoId'])
        
        # Find videos that need metadata
        new_videos = video_ids - existing_metadata - set(self.metadata_cache.keys())
        
        if new_videos:
            print(f"Fetching metadata for {len(new_videos)} new videos...")
            new_metadata = self._get_video_metadata(list(new_videos))
            
            # Combine new metadata with cached metadata
            all_metadata = list(self.metadata_cache.values())
            
            # Save to CSV
            metadata_df = pd.DataFrame(all_metadata)
            metadata_df = metadata_df[[
                'videoId', 'duration', 'publishedAt', 'title', 'channelId', 'channelTitle',
                'categoryId', 'definition', 'viewCount', 'likeCount',
                'commentCount', 'favoriteCount'
            ]]
            metadata_df.to_csv(output_csv, index=False)
            
            # Update cache
            self._save_cache()
            
            print(f"Updated metadata saved to {output_csv}")
        else:
            print("No new videos to process")

def main():
    # File paths
    history_csv = os.path.join(pocket_secrets.data_folder, 'youtube_history.csv')
    metadata_csv = os.path.join(pocket_secrets.data_folder, 'youtube_metadata.csv')
    
    try:
        fetcher = YouTubeMetadataFetcher(pocket_secrets.google_api_key)
        fetcher.process_history(history_csv, metadata_csv)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()