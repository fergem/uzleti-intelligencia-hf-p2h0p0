# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_titles", "scrape_headings"]

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

# Load cancelled shows
cancelled_shows = pd.read_csv(netflix_canceled_shows_path)
cancelled_shows['title_cleaned'] = cancelled_shows['Title'].apply(normalize_and_clean_title)
cancelled_titles_set = set(cancelled_shows['title_cleaned'])

df = pd.read_json(FILE_TO_CLEAN_PATH)

# Keep only the columns we need
columns_to_keep = ['title', 'type', 'titlereleased', 'netflixid', 'category', 'date_released']
df = df[columns_to_keep]

# Remove unwanted text from title and narrow date 
df = df[~df['titlereleased'].str.contains("Limited Series", na=False)]
df['title'] = df['title'].apply(lambda x: x.rstrip() if isinstance(x, str) else x)
df['title_cleaned'] = df['title'].apply(normalize_and_clean_title)
df['date_released'] = df['date_released'].apply(lambda x: x[:4] if isinstance(x, str) else x)
df['titlereleased'] = df.apply(
    lambda row: row['date_released'] if isinstance(row['titlereleased'], str) and row['titlereleased'].strip() == "" else row['titlereleased'],
    axis=1
)
df['titlereleased'] = df['titlereleased'].apply(lambda x: x[:4] if isinstance(x, str) else x)

df['category'] = df['category'].apply(normalize_genre)

# Set to cancelled if title is in cancelled shows
df['cancelled'] = df['title_cleaned'].apply(lambda x: x in cancelled_titles_set)

# Rename columns to match IMDb data
df['type'] = df['type'].apply(lambda x: 'movie' if x == "Movie" else 'tvSeries' if x == "TV" else x)

df.to_json(CLEANED_FILE_PATH, orient='records', indent=4, force_ascii=False)

#os.remove(FILE_TO_CLEAN_PATH)
