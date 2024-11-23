# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_titles"]

# -

import os
import re
import unicodedata
import pandas as pd

file_path = 'etl/datasets/imdb_titles.tsv'
ratings_file = 'etl/datasets/imdb_ratings.tsv'

def normalize_and_clean_title(title):
    if not isinstance(title, str):
        return ""
    title = unicodedata.normalize('NFD', title).encode('ascii', 'ignore').decode('ascii')
    title = title.lower()
    title = re.sub(r'[^\w]', '', title)
    title.strip()
    title = title.replace("theseries", "").replace("themovie", "")
    return title


cleaned_file_path = 'etl/datasets/imdb_titles_cleaned.csv'

df = pd.read_csv(file_path, sep='\t')

unwanted_types = ['short', 'videoGame', 'tvEpisode', 'tvMiniSeries', 'tvSpecial', 'tvShort', 'tvMovie', 'video']
df = df[~df['titleType'].isin(unwanted_types)]

excluded_genres = ["Talk-Show", "Short", "News", "Sport"]
df = df[~df['genres'].str.contains("|".join(excluded_genres), na=False, case=False)]

df = df[pd.to_numeric(df['startYear'], errors='coerce') >= 1980]

df = df.drop(columns=['originalTitle', 'isAdult', 'endYear'])

# +
df['primaryTitle_cleaned'] = df['primaryTitle'].apply(normalize_and_clean_title)
# -


ratings_df = pd.read_csv(ratings_file, sep='\t', dtype=str)
ratings_df = ratings_df.drop(columns=['numVotes'])

imdb_data = pd.merge(df, ratings_df, on="tconst", how="left")
imdb_data = imdb_data.dropna()
imdb_data.to_csv(cleaned_file_path, sep=",", index=False)

os.remove(file_path)
os.remove(ratings_file)

