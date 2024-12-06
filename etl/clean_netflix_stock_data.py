# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_stock_data"]

# -

import json
import os
import pandas as pd

FILE_TO_CLEAN_PATH = 'etl/datasets/netflix_stock_data.json'
CLEANED_FILE_PATH = 'etl/datasets/netflix_stock_data_cleaned.json'

def load_stock_data(file_path):
    return pd.read_json(file_path)

def clean_stock_data(df):
    df_cleaned = df.apply(lambda x: {
        "timeRange": x[0].get('timeRange', None),
        "symbol": x[0].get('symbol', None),
        "name": x[0].get('allSymbols', [{}])[0].get('name', None) if x[0].get('allSymbols') else None,
        "priceBars": x[0].get('priceBars', None)
    }).iloc[0]
    return df_cleaned

def save_cleaned_data(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Cleaned data saved to {file_path}")

def main():
    df = load_stock_data(FILE_TO_CLEAN_PATH)
    cleaned_data = clean_stock_data(df)
    save_cleaned_data(cleaned_data, CLEANED_FILE_PATH)

if __name__ == "__main__":
    main()

#os.remove(FILE_TO_CLEAN_PATH)