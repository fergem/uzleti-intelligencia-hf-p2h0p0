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
EXTRACTED_FILE = os.path.join(OUTPUT_DIR, "imdb_ratings.tsv")

def create_output_dir(directory):
    os.makedirs(directory, exist_ok=True)

def download_file(url, file_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"Downloaded file to {file_path}")

def extract_gzip_file(compressed_file, extracted_file):
    with gzip.open(compressed_file, "rb") as f_in:
        with open(extracted_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Extracted file to {extracted_file}")

def remove_file(file_path):
    os.remove(file_path)
    print(f"Removed temporary file: {file_path}")

def download_and_extract():
    create_output_dir(OUTPUT_DIR)
    download_file(URL, COMPRESSED_FILE)
    extract_gzip_file(COMPRESSED_FILE, EXTRACTED_FILE)
    remove_file(COMPRESSED_FILE)

if __name__ == "__main__":
    download_and_extract()