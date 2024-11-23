# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

import os
import requests
import json

URL = "https://www.whats-on-netflix.com/wp-content/plugins/whats-on-netflix/json/originals.json"
OUTPUT_DIR = "etl/datasets"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "netflix_originals.json")

def download_json():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Downloading file...")
    try:
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        
        # Parse the JSON to decode escape sequences
        data = response.json()
        
        # Write the decoded JSON data to the file
        with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        
        print(f"Downloaded and saved file to {OUTPUT_FILE}")
    
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_json()
