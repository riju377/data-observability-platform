
import os
import json
import logging

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Please install google-auth-oauthlib: pip install google-auth-oauthlib")
    exit(1)

# Scopes required for sending logic
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def main():
    print("=== Gmail API Refresh Token Generator ===")
    print("This script will help you generate a Refresh Token for your Render app.")
    print("1. Make sure you have 'credentials.json' (Client Secret JSON) in this folder.")
    print("   (Download it from Google Cloud Console -> APIs & Services -> Credentials)")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(script_dir, 'credentials.json')

    if not os.path.exists(credentials_path):
        print(f"\nERROR: 'credentials.json' not found at {credentials_path}!")
        print("Please place the file in the same directory as this script and run it again.")
        return

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_path, SCOPES)
        
        print("\nLaunching browser for authentication...")
        creds = flow.run_local_server(port=0)
        
        print("\n=== SUCCESS! ===")
        print("Here are your credentials for Render:")
        print("-" * 60)
        
        # Load client info to print nicely
        with open(credentials_path) as f:
            client_config = json.load(f)
            # It might be 'installed' or 'web'
            config = client_config.get('installed') or client_config.get('web')
            
        print(f"GMAIL_CLIENT_ID={config['client_id']}")
        print(f"GMAIL_CLIENT_SECRET={config['client_secret']}")
        print(f"GMAIL_REFRESH_TOKEN={creds.refresh_token}")
        print("-" * 60)
        print("Copy these 3 values and add them to your Render Environment Variables.")
        
    except Exception as e:
        print(f"\nERROR: {e}")

if __name__ == '__main__':
    logging.getLogger('google_auth_oauthlib.flow').setLevel(logging.ERROR)
    main()
