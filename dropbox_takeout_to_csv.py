import json
import csv
import zipfile
from datetime import datetime
import os
from pathlib import Path
import glob
import pickle
import pocket_secrets

def extract_video_id(url: str) -> str:
    """Extract video ID from YouTube URL."""
    if 'watch?v=' in url:
        return url.split('watch?v=')[1].split('&')[0]
    return url.split('/')[-1]

def determine_source(url: str) -> str:
    """Determine if the video was watched on YouTube or YouTube Music."""
    return "music" if "music.youtube.com" in url else "youtube"

def find_latest_takeout(dropbox_dir):
    """
    Find all unprocessed takeout files in the specified directory.
    
    Args:
        dropbox_dir (str): Directory to search for takeout files
        
    Returns:
        list: List of paths to unprocessed takeout files
    """
    # Find all takeout zip files
    pattern = os.path.join(dropbox_dir, 'takeout-*.zip')
    takeout_files = glob.glob(pattern)
    
    # Load processed files list
    processed_files_path = os.path.join(dropbox_dir, 'processed_takeouts.pkl')
    try:
        with open(processed_files_path, 'rb') as f:
            processed_files = pickle.load(f)
    except FileNotFoundError:
        processed_files = set()
    
    # Filter out already processed files
    unprocessed_files = [f for f in takeout_files if f not in processed_files]
    return unprocessed_files, processed_files, processed_files_path

def process_youtube_history(zip_path, output_csv_path):
    """
    Extract YouTube watch history from Google Takeout zip file and convert to CSV.
    
    Args:
        zip_path (str): Path to the Google Takeout zip file
        output_csv_path (str): Path where the output CSV should be saved
    """
    # Find and extract the YouTube history JSON file from the zip
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Look for the watch-history.json file in the zip
        json_path = None
        for file in zip_ref.namelist():
            if file.endswith('watch-history.json'):
                json_path = file
                break
        
        if not json_path:
            raise FileNotFoundError("Could not find watch-history.json in the zip file")
        
        # Extract the JSON file
        temp_dir = os.path.join(os.path.dirname(zip_path), '.temp_extract')
        os.makedirs(temp_dir, exist_ok=True)
        zip_ref.extract(json_path, path=temp_dir)
    
    # Read the JSON file
    json_file_path = os.path.join(temp_dir, json_path)
    with open(json_file_path, 'r', encoding='utf-8') as f:
        history_data = json.load(f)
    
    # Write to CSV
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'videoId', 'source'])  # Write headers
        
        for entry in history_data:
            if 'titleUrl' in entry and 'time' in entry:
                url = entry['titleUrl']
                video_id = extract_video_id(url)
                source = determine_source(url)
                time = entry['time']
                writer.writerow([time, video_id, source])
    
    # Clean up: remove the temporary directory and its contents
    import shutil
    shutil.rmtree(temp_dir)
    
    return True

def main():
    # Get the user's home directory
    home_dir = str(Path.home())
    
    # Assuming the Dropbox folder is in the default location
    dropbox_dir = os.path.join(home_dir, 'Dropbox/Apps/Google Download Your Data')
    output_csv = os.path.join(pocket_secrets.data_folder, 'youtube_history.csv')
    
    try:
        # Find unprocessed takeout files
        unprocessed_files, processed_files, processed_files_path = find_latest_takeout(dropbox_dir)
        
        if not unprocessed_files:
            print("No new takeout files to process")
            return
        
        # Sort files by timestamp in filename (newest first)
        unprocessed_files.sort(reverse=True)
        
        # Process the newest file
        latest_file = unprocessed_files[0]
        print(f"Processing latest takeout file: {latest_file}")
        
        if process_youtube_history(latest_file, output_csv):
            # Update processed files list
            processed_files.add(latest_file)
            with open(processed_files_path, 'wb') as f:
                pickle.dump(processed_files, f)
            
            print(f"Successfully created CSV file at: {output_csv}")
            
            # Optionally, remove old takeout files to save space
            # for old_file in unprocessed_files[1:]:
            #     try:
            #         os.remove(old_file)
            #         print(f"Removed old takeout file: {old_file}")
            #     except OSError as e:
            #         print(f"Error removing old file {old_file}: {e}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()