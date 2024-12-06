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

def create_output_dir(directory):
    os.makedirs(directory, exist_ok=True)

def fetch_json(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.json()

def save_json_to_file(data, file_path):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

def download_json():
    create_output_dir(OUTPUT_DIR)
    print("Downloading file...")
    try:
        data = fetch_json(URL)
        save_json_to_file(data, OUTPUT_FILE)
        print(f"Downloaded and saved file to {OUTPUT_FILE}")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_json()