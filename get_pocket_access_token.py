import requests
import webbrowser
import time

class PocketAuth:
    def __init__(self, consumer_key: str):
        self.consumer_key = consumer_key
        self.base_url = "https://getpocket.com/v3"
        
    def get_request_token(self) -> str:
        """Get a request token from Pocket."""
        response = requests.post(
            f"{self.base_url}/oauth/request",
            json={
                "consumer_key": self.consumer_key,
                "redirect_uri": "http://localhost"
            },
            headers={"X-Accept": "application/json"}
        )
        response.raise_for_status()
        return response.json()["code"]
        
    def authorize_request_token(self, request_token: str):
        """Open the authorization URL in a browser."""
        auth_url = (
            f"https://getpocket.com/auth/authorize"
            f"?request_token={request_token}"
            f"&redirect_uri=http://localhost"
        )
        print("\nOpening your browser to authorize the application...")
        print("Please log in to Pocket and authorize the application.")
        print("After authorizing, come back here and press Enter to continue.")
        webbrowser.open(auth_url)
        input("\nPress Enter after you've authorized the application: ")
        
    def get_access_token(self, request_token: str) -> str:
        """Convert request token to access token."""
        response = requests.post(
            f"{self.base_url}/oauth/authorize",
            json={
                "consumer_key": self.consumer_key,
                "code": request_token
            },
            headers={"X-Accept": "application/json"}
        )
        response.raise_for_status()
        return response.json()["access_token"]

def main():
    print("Pocket Authentication Helper")
    print("-" * 50)
    print("\nFirst, you need a consumer key. If you don't have one:")
    print("1. Go to https://getpocket.com/developer/")
    print("2. Click 'Create New App'")
    print("3. Fill out the form (Platform: 'Desktop'; Permissions: 'Retrieve')")
    print("4. Submit the form and copy the consumer key")
    
    consumer_key = input("\nEnter your consumer key: ").strip()
    
    try:
        auth = PocketAuth(consumer_key)
        
        # Get request token
        print("\nGetting request token...")
        request_token = auth.get_request_token()
        
        # Authorize the token
        auth.authorize_request_token(request_token)
        
        # Get access token
        print("\nGetting access token...")
        access_token = auth.get_access_token(request_token)
        
        print("\nSuccess! Here are your credentials:\n")
        print(f"Consumer Key: {consumer_key}")
        print(f"Access Token: {access_token}")
        print("\nTo use these with the Pocket exporter, set these environment variables:")
        print(f"\nexport POCKET_CONSUMER_KEY='{consumer_key}'")
        print(f"export POCKET_ACCESS_TOKEN='{access_token}'")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()