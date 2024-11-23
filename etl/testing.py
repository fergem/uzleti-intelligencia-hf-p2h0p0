import pandas as pd


netflix_originals_file_path = "etl/datasets/netflix_originals_cleaned.json"
imdb_titles_file_path =  "etl/datasets/imdb_titles_cleaned.csv"

netflix_originals = pd.read_json(netflix_originals_file_path)
imdb_titles = pd.read_csv(imdb_titles_file_path)

# Find titles in netflix_originals not in imdb_titles
missing_titles = netflix_originals[~netflix_originals['title_cleaned'].isin(imdb_titles['primaryTitle_cleaned'])]

# Save the missing titles to a CSV file
output_file_path = "missing_netflix_titles.csv"
missing_titles.to_csv(output_file_path, index=False)

print(f"Missing Netflix originals saved to: {output_file_path}")
