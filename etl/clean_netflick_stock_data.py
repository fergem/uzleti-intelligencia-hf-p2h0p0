# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["gather_netflix_stock_data"]

# -

import json
import os
import pandas as pd

FILE_TO_CLEAN_PATH = 'etl/datasets/netflix_stock_data.json'
CLEANED_FILE_PATH = 'etl/datasets/netflix_stock_data_cleaned.json'

df = pd.read_json(FILE_TO_CLEAN_PATH)

df_cleaned = df.apply(lambda x: {
    "timeRange": x[0].get('timeRange', None),
    "symbol": x[0].get('symbol', None),
    "name": x[0].get('allSymbols', [{}])[0].get('name', None) if x[0].get('allSymbols') else None,
    "priceBars": x[0].get('priceBars', None)
}).iloc[0] 

with open(CLEANED_FILE_PATH, 'w') as f:
    json.dump(df_cleaned, f, indent=4)

#os.remove(FILE_TO_CLEAN_PATH)0