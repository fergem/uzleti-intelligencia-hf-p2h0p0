# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_titles"]

# -

import os
import re
import unicodedata
import pandas as pd

def normalize_genre(genre):
    genres_parsed = [g.strip() for g in genre.split(",")] if genre else []
    
    genre_mapping = {
        'Drama': ['Drama', 'Dramas'],
        'Comedy': ['Comedy', 'Stand-up Comedy', 'Stand-up', 'Standup'],
        'Action': ['Action'],
        'Romance': ['Romance', 'Romantic'],
        'Sci-Fi/Fantasy': ['Sci-Fi', 'Sci-fi', 'Fantasy'],
        'Thriller': ['Thriller', 'Thrillers'],
        'Horror': ['Horror'],
        'Mystery': ['Mystery'],
        'Crime': ['Crime', 'True Crime', 'True-crime', 'Con-Artist'],
        'Documentary': ['Documentary', 'Docuseries', 'Docudrama', 'Behind the Scenes', 'Making-of', 'Making-Of'],
        'Biographical': ['Biography'],
        'Historical': ['History', 'Historical'],
        'Family/Children': ['Family', 'Kids', 'Children'],
        'Animation': ['Animation', 'Animated', 'Cartoon', 'Anime'],
        'Adventure': ['Adventure'],
        'Teen': ['Teen'],
        'Medical': ['Medical'],
        'LGBTQ': ['LGBTQ'],
        'Stand-up Comedy': ['Stand-up', 'Standup', 'Stand-up Special'],
        'Sports': ['Sport', 'Boxing', 'Sports'],
        'Reality TV': ['Reality-TV', 'Reality TV', 'Reality'],
        'Game Show': ['Game-Show'],
        'Talk Show': ['Talk Show', 'Talk Shows'],
        'Variety': ['Variety'],
        'Political': ['Political'],
        'Music': ['Music'],
        'Specials': ['Special', 'Making-of', 'Behind-the-scenes'],
        'Regional': ['Bollywood', 'Nollywood', 'Spanish', 'French', 'Latin American TV'],
        'Food/Travel': ['Food', 'Travel'],
        'Courtroom': ['Courtroom'],
        'Movies/TV': ['Movies', 'TV'],
        'Stories/BLM': ['Stories', 'BLM'],
        'Zombie': ['Zombie'],
        'Korean': ['Korean', 'K-Drama'],
    }
    
    if not genres_parsed:
        return 'Other'
    
    categories_of_genre = set()
    
    for category, keywords in genre_mapping.items():
        for g in genres_parsed:
            if g in keywords:
                categories_of_genre.add(category)
    
    return ",".join(categories_of_genre) if categories_of_genre else "Other"


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
df['genres'] = df['genres'].apply(normalize_genre)

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

#os.remove(file_path)
#os.remove(ratings_file)

