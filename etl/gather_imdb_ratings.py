# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

import os
import requests
import gzip
import shutil

URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
OUTPUT_DIR = "etl/datasets"
COMPRESSED_FILE = "title.ratings.tsv.gz"

def download_and_extract():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Downloading file...")
    response = requests.get(URL, stream=True)
    response.raise_for_status()
    
    with open(COMPRESSED_FILE, "wb") as file:
        file.write(response.content)
    print(f"Downloaded file to {COMPRESSED_FILE}")
    
    extracted_file_path = os.path.join(OUTPUT_DIR, "imdb_ratings.tsv")
    with gzip.open(COMPRESSED_FILE, "rb") as f_in:
        with open(extracted_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Extracted file to {extracted_file_path}")
    
    os.remove(COMPRESSED_FILE)
    print(f"Removed temporary file: {COMPRESSED_FILE}")


if __name__ == "__main__":
    download_and_extract()
