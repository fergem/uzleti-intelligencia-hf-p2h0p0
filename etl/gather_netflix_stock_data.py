# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

import os
import requests
import json

URL = "https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22NFLX%22%2C%22timeRange%22%3A%225Y%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D"
OUTPUT_DIR = "etl/datasets"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "netflix_stock_data.json")

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


    
