# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_titles", "scrape_headings"]

# -

import os
import re
import unicodedata
import pandas as pd

FILE_TO_CLEAN_PATH = 'etl/datasets/netflix_originals.json'
CLEANED_FILE_PATH = 'etl/datasets/netflix_originals_cleaned.json'
netflix_canceled_shows_path = "etl/datasets/netflix_cancelled_shows.csv"

def normalize_and_clean_title(title):
    if not isinstance(title, str):
        return ""
    title = unicodedata.normalize('NFD', title).encode('ascii', 'ignore').decode('ascii')
    title = title.lower()
    title = re.sub(r'[^\w]', '', title)
    title.strip()
    title = title.replace("theseries", "").replace("themovie", "")
    return title

cancelled_shows = pd.read_csv(netflix_canceled_shows_path)
cancelled_shows['title_cleaned'] = cancelled_shows['Title'].apply(normalize_and_clean_title)
cancelled_titles_set = set(cancelled_shows['title_cleaned'])

df = pd.read_json(FILE_TO_CLEAN_PATH)

columns_to_keep = ['title', 'type', 'titlereleased', 'netflixid', 'category', 'date_released']
df = df[columns_to_keep]

df = df[~df['titlereleased'].str.contains("Limited Series", na=False)]
df['title'] = df['title'].apply(lambda x: x.rstrip() if isinstance(x, str) else x)
df['title_cleaned'] = df['title'].apply(normalize_and_clean_title)
df['date_released'] = df['date_released'].apply(lambda x: x[:4] if isinstance(x, str) else x)
df['titlereleased'] = df.apply(
    lambda row: row['date_released'] if isinstance(row['titlereleased'], str) and row['titlereleased'].strip() == "" else row['titlereleased'],
    axis=1
)
df['titlereleased'] = df['titlereleased'].apply(lambda x: x[:4] if isinstance(x, str) else x)

df['cancelled'] = df['title_cleaned'].apply(lambda x: x in cancelled_titles_set)
df['type'] = df['type'].apply(lambda x: 'movie' if x == "Movie" else 'tvSeries' if x == "TV" else x)

df.to_json(CLEANED_FILE_PATH, orient='records', indent=4, force_ascii=False)

os.remove(FILE_TO_CLEAN_PATH)
