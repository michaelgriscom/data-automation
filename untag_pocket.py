import requests
import pocket_secrets

# Replace with your Pocket consumer key and access token
POCKET_CONSUMER_KEY = pocket_secrets.consumer_key
POCKET_ACCESS_TOKEN = pocket_secrets.access_token

# Pocket API endpoints
POCKET_GET_URL = 'https://getpocket.com/v3/get'
POCKET_MODIFY_URL = 'https://getpocket.com/v3/send'

def get_favorited_items():
    params = {
        'consumer_key': POCKET_CONSUMER_KEY,
        'access_token': POCKET_ACCESS_TOKEN,
        'favorite': '1',
        'state': 'unread',
        'detailType': 'complete'
    }
    response = requests.post(POCKET_GET_URL, json=params)
    response.raise_for_status()
    return response.json().get('list', {}).values()

def modify_items_in_bulk(actions):
    params = {
        'consumer_key': POCKET_CONSUMER_KEY,
        'access_token': POCKET_ACCESS_TOKEN,
        'actions': actions
    }
    response = requests.post(POCKET_MODIFY_URL, json=params)
    response.raise_for_status()

def clean_and_unfavorite_items():
    while True:
        items = get_favorited_items()
        if not items:
            break

        actions = []

        for item in items:
            item_id = item['item_id']
            existing_tags = item.get('tags', {})

            # Filter out tags starting with '*'
            new_tags = [tag for tag in existing_tags if not tag.startswith('*')]

            # Create a combined action for untagging and unfavoriting
            actions.append({
                'action': 'tags_replace',
                'item_id': item_id,
                'tags': ','.join(new_tags)
            })
            actions.append({
                'action': 'unfavorite',
                'item_id': item_id
            })

        if actions:
            modify_items_in_bulk(actions)
            print(f"Processed {len(items)} items: Updated tags and unfavorited.")
        else:
            break

if __name__ == "__main__":
    clean_and_unfavorite_items()
