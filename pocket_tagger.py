import csv
import os
from typing import List, Dict, Set
import requests
from newspaper import Article
from pocket import Pocket
import re
from time import sleep
from urllib.parse import urlparse
import pocket_secrets

def load_tags(csv_path) -> Set[str]:
    """Load tags from CSV file and return as a set."""
    tags = set()
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(f"Loaded {row} tag")
            tags.add(row['tag'].lower().strip())
    return tags

def extract_article_text(url: str, max_retries: int = 3) -> str:
    """Extract article text using newspaper3k with retry logic."""
    for attempt in range(max_retries):
        try:
            article = Article(url)
            article.download()
            article.parse()
            return article.text.lower()
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to extract text from {url}: {str(e)}")
                return ""
            sleep(1)

def find_matching_tags(text: str, tags: Set[str]) -> Set[str]:
    """Find tags that appear in the text with word boundary checking."""
    matching_tags = set()
    for tag in tags:
        # Create regex pattern with word boundaries
        pattern = r'\b' + re.escape(tag) + r'\b'
        if re.search(pattern, text):
            matching_tags.add('*'+ tag)
    return matching_tags

def get_pocket_items(consumer_key: str, access_token: str) -> Dict:
    """Get unread and unfavorited items from Pocket."""
    url = "https://getpocket.com/v3/get"
    headers = {'Content-Type': 'application/json'}
    data = {
        'consumer_key': consumer_key,
        'access_token': access_token,
        'state': 'unread',
        'favorite': '0',
        'detailType': 'complete'
    }
    
    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()
    return response.json()

def send_batch_actions(consumer_key: str, access_token: str, actions: List[Dict]) -> None:
    """Send batch actions to Pocket modify endpoint."""
    if not actions:
        return
        
    url = "https://getpocket.com/v3/send"
    headers = {'Content-Type': 'application/json'}
    data = {
        'consumer_key': consumer_key,
        'access_token': access_token,
        'actions': actions
    }
    
    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()
    print(f"Successfully processed batch of {len(actions)} actions")

def process_pocket_items(consumer_key: str, access_token: str, tags: Set[str]) -> None:
    """Process unread and unfavorited Pocket items."""
    while True:
        try:
            # Get unread and unfavorited items
            response = get_pocket_items(consumer_key, access_token)
            print(f"Fetched {len(response['list'].items())} items")

            if not response.get('list'):
                print("No more items to process")
                break
            
            # Prepare batch actions
            actions = []
            
            for item_id, item in response['list'].items():
                # Skip items without a valid URL
                if 'resolved_url' not in item or not item['resolved_url']:
                    continue
                    
                url = item['resolved_url']
                
                # Skip if URL is not valid
                try:
                    parsed = urlparse(url)
                    if not all([parsed.scheme, parsed.netloc]):
                        continue
                except Exception:
                    continue
                
                print(f"Processing {url}")

                # Mark article as favorite (i.e. tagged)
                actions.append({
                    'action': 'favorite',
                    'item_id': item_id
                })

                # Extract and process text
                article_text = extract_article_text(url)
                    
                # Find matching tags
                matching_tags = find_matching_tags(article_text, tags)

                if matching_tags:
                    print(f"Found tags: {matching_tags}")
                    
                    # Add tags action
                    actions.append({
                        'action': 'tags_add',
                        'item_id': item_id,
                        'tags': ','.join(matching_tags)
                    })
            
            # Send batch actions
            if actions:
                send_batch_actions(consumer_key, access_token, actions)
                sleep(1)  # Rate limiting
                
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            break

def main():
    tag_csv_path = os.path.join(pocket_secrets.data_folder, 'pocket_tags.csv')
    # Load tags
    tags = load_tags(tag_csv_path)
    print(f"Loaded {len(tags)} tags")
    
    # Process items
    process_pocket_items(pocket_secrets.consumer_key, pocket_secrets.access_token, tags)

if __name__ == "__main__":
    main()