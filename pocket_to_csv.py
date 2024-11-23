import requests
import csv
import os
from typing import Dict, List, Optional, Tuple
import tempfile
import shutil
import time
import pocket_secrets

class PocketExporter:
    def __init__(self, consumer_key: str, access_token: str, csv_path: str):
        self.consumer_key = consumer_key
        self.access_token = access_token
        self.csv_path = csv_path
        self.api_url = "https://getpocket.com/v3"
        self.last_update_file = f"{csv_path}.timestamp"
        self.items_per_page = 30  # Pocket API default
        
    def get_last_update_time(self) -> Optional[int]:
        """Get the timestamp of the last successful update."""
        if os.path.exists(self.last_update_file):
            with open(self.last_update_file, 'r') as f:
                return int(f.read().strip())
        return None
        
    def save_last_update_time(self, timestamp: int):
        """Save the timestamp of the current successful update."""
        with open(self.last_update_file, 'w') as f:
            f.write(str(timestamp))
        
    def get_existing_items(self) -> Dict[str, Dict]:
        """Read existing items from CSV file and return them as a dictionary."""
        if not os.path.exists(self.csv_path):
            return {}
            
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)
            return {row['Item ID']: row for row in reader}
             
    def fetch_page(self, since: Optional[int], offset: int) -> Tuple[List[Dict], bool]:
        """
        Fetch a single page of items from Pocket API.
        Returns tuple of (items, has_more) where has_more indicates if there are more pages.
        """
        params = {
            'consumer_key': self.consumer_key,
            'access_token': self.access_token,
            'detailType': 'complete',
            'state': 'all',
            'count': self.items_per_page,
            'offset': offset
        }
        
        if since:
            params['since'] = since
            
        response = requests.post(f"{self.api_url}/get", json=params)
        response.raise_for_status()
        
        data = response.json()
        items = data.get('list', {})
        
        # Convert items dict to list and format each item
        formatted_items = [self.format_item(item) for item in items.values()]
        
        # Check if there are more items
        # If we got a full page of items, there might be more
        has_more = len(formatted_items) >= self.items_per_page
        
        return formatted_items, has_more
            
    def fetch_new_items(self, since: Optional[int] = None) -> List[Dict]:
        """Fetch items from Pocket API."""
        # Get the last update time
        since = self.get_last_update_time()
        current_time = int(time.time())
        
        all_items = []
        offset = 0
        has_more = True
        
        while has_more:
            items, has_more = self.fetch_page(since, offset)
            all_items.extend(items)
            offset += len(items)
            
            # Log progress
            if has_more:
                print(f"Fetched {len(items)} items (total so far: {len(all_items)})")
        
        # Save the current timestamp if we got any items
        if all_items:
            self.save_last_update_time(current_time)
            print(f"Fetched a total of {len(all_items)} items")
            
        return all_items
        
    def clean_text(self, text: str) -> str:
        """Clean text fields by removing null bytes and normalizing whitespace."""
        if not isinstance(text, str):
            return str(text)
        # Remove null bytes
        text = text.replace('\x00', '')
        # Replace multiple whitespace with single space
        text = ' '.join(text.split())
        return text
        
    def format_item(self, item: Dict) -> Dict:
        """Format Pocket item data according to CSV structure."""
        # Format tags as comma-separated string
        tags = item.get('tags', {})
        tags_string = ';'.join(sorted(tags.keys())) if tags else ''
        
        formatted = {
            'Item ID': item.get('item_id', ''),
            'Resolved ID': item.get('resolved_id', ''),
            'Given URL': item.get('given_url', ''),
            'Given Title': self.clean_text(item.get('given_title', '')),
            'Favorite': '1' if item.get('favorite') == '1' else '0',
            'Status': item.get('status', ''),
            'Resolved Title': self.clean_text(item.get('resolved_title', '')),
            'Resolved URL': item.get('resolved_url', ''),
            'Is Article': '1' if item.get('is_article') == '1' else '0',
            'Has Video': '1' if item.get('has_video') == '1' else '0',
            'Is Index': '1' if item.get('is_index') == '1' else '0',
            'Word Count': item.get('word_count', ''),
            'Listen Duration Estimate': item.get('listen_duration_estimate', ''),
            'Time Added': item.get('time_added', 0),
            'Time Updated': item.get('time_updated', 0),
            'Time Read': item.get('time_read', ''),
            'Time Favorited': item.get('time_favorited', ''),
            'Tags': tags_string
        }
        
        # Ensure all values are strings
        return {k: str(v) for k, v in formatted.items()}
        
    def export_to_csv(self, items: List[Dict]):
        """Export items to CSV file, replacing existing entries with updated ones."""
        headers = [
            'Item ID', 'Resolved ID', 'Given URL', 'Given Title', 'Favorite',
            'Status', 'Resolved Title', 'Resolved URL', 'Is Article', 'Has Video',
            'Is Index', 'Word Count', 'Listen Duration Estimate', 'Time Added',
            'Time Updated', 'Time Read', 'Time Favorited', 'Tags'
        ]
        
        # Get existing items
        existing_items = self.get_existing_items()
        
        # Update existing items with new data
        for item in items:
            existing_items[item['Item ID']] = item
            
        # Write all items to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', newline='') as temp_file:
            writer = csv.DictWriter(temp_file, 
                                  fieldnames=headers, 
                                  delimiter=',', 
                                  quoting=csv.QUOTE_ALL,
                                  quotechar='"',
                                  escapechar='\\')
            writer.writeheader()
            writer.writerows(existing_items.values())
            
        # Replace the original file with the temporary file
        shutil.move(temp_file.name, self.csv_path)
            
    def run(self):
        """Main execution method."""
        try:
            # Fetch new items
            new_items = self.fetch_new_items()
            
            if new_items:
                self.export_to_csv(new_items)
                print(f"Successfully processed {len(new_items)} items in {self.csv_path}")
            else:
                print("No new items to process")
                
        except Exception as e:
            print(f"Error: {str(e)}")
            raise

if __name__ == "__main__":
    # Load configuration from environment variables
    consumer_key = pocket_secrets.consumer_key
    access_token = pocket_secrets.access_token
    csv_path = os.path.join(pocket_secrets.data_folder, 'pocket_items.csv')
    
    exporter = PocketExporter(consumer_key, access_token, csv_path)
    exporter.run()