# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["clean_netflix_titles", "clean_imdb_titles"]

# -

import time
from sqlalchemy import BigInteger, create_engine, Column, Integer, String, Float, ForeignKey, Table, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import pandas as pd

Base = declarative_base()

# +
movie_categories = Table(
    'movie_categories', Base.metadata,
    Column('movie_id', Integer, ForeignKey('movies.id'), primary_key=True),
    Column('category_id', Integer, ForeignKey('categories.id'), primary_key=True)
)

# +
class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    type = Column(String)
    released_year = Column(Integer)
    runtime_minutes = Column(Integer, nullable=True)
    imdb_rating = Column(Float, nullable=True)
    netflix_id = Column(Integer, nullable=True)
    cancelled = Column(Boolean, default=False)
    categories = relationship('Category', secondary=movie_categories, back_populates='movies')

# +
class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    movies = relationship('Movie', secondary=movie_categories, back_populates='categories')

class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    stock_prices = relationship('StockPrice', back_populates='company')

# StockPrice table model
class StockPrice(Base):
    __tablename__ = 'stock_prices'
    id = Column(Integer, primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    trade_time = Column(String, nullable=False)
    trade_time_mills = Column(BigInteger, nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    company = relationship('Company', back_populates='stock_prices')

# -

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/movies_dev_db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# +
netflix_originals_file_path = "etl/datasets/netflix_originals_cleaned.json"
imdb_titles_file_path =  "etl/datasets/imdb_titles_cleaned.csv"
netflix_stock_data_path =  "etl/datasets/netflix_stock_data_cleaned.json"

# +
def clear_data(session):
    try:
        session.execute(movie_categories.delete())
        session.query(StockPrice).delete()
        session.query(Company).delete()
        session.query(Movie).delete()
        session.query(Category).delete()
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error during data clearing: {e}")

def load_stock_data(session):
    try:
        netflix_stock_data = pd.read_json(netflix_stock_data_path)

        company = Company(
            symbol=netflix_stock_data["symbol"][0],
            name=netflix_stock_data["name"][0]
        )

        session.add(company)
        session.commit()

        stock_prices_to_insert = []
        for price_data in netflix_stock_data["priceBars"]:
            stock_price = StockPrice(
                open = float(price_data["open"]) if price_data["open"] is not None else 0.0,
                high = float(price_data["high"]) if price_data["high"] is not None else 0.0,
                low = float(price_data["low"]) if price_data["low"] is not None else 0.0,
                close = float(price_data["close"]) if price_data["close"] is not None else 0.0,
                volume = int(price_data["volume"]) if price_data["volume"] is not None else 0,
                trade_time = price_data["tradeTime"] if price_data["tradeTime"] is not None else "",
                trade_time_mills = int(price_data["tradeTimeinMills"]) if price_data["tradeTimeinMills"] is not None else 0,
                company_id=company.id
            )
            stock_prices_to_insert.append(stock_price)
        
        session.bulk_save_objects(stock_prices_to_insert, return_defaults=False)
        session.commit()

    except Exception as e:
        session.rollback()
        print(f"Error while loading stock data: {e}")
    finally:
        print("Stock data loaded successfully.")

def load_imdb_data(session, netflix_originals, category_mapping):
    imdb_titles = pd.read_csv(imdb_titles_file_path, chunksize=10000)
    chunk_number = 0
    for chunk in imdb_titles:
        chunk_start_time = time.time()
        chunk_number += 1
        print(f"Processing chunk {chunk_number}...")
    
        movies_to_insert = []
        categories_to_insert = []
        for _, row in chunk.iterrows():
            title_cleaned = row['primaryTitle_cleaned']
            matching_netflix = netflix_originals[(netflix_originals['title_cleaned'] == title_cleaned) &
                                                    (netflix_originals['titlereleased'] == row['startYear']) & 
                                                    (netflix_originals['type'] == row['titleType'])]


            imdb_rating = None if row['averageRating'] == "\\N" else float(row['averageRating']) if row['averageRating'] else None
            runtime_minutes = None if row['runtimeMinutes'] == "\\N" else int(row['runtimeMinutes']) if row['runtimeMinutes'] else None
            start_year = None if row['startYear'] == "\\N" else int(row['startYear'])
        
            if not matching_netflix.empty:
                netflix_row = matching_netflix.iloc[0]
                movie = Movie(
                    title=row['primaryTitle'].strip(),
                    type=row['titleType'],
                    released_year=start_year,
                    runtime_minutes=runtime_minutes,
                    imdb_rating=imdb_rating,
                    netflix_id=int(netflix_row['netflixid']) if 'netflixid' in netflix_row else None,
                    cancelled=netflix_row['cancelled']
                )
            else:
                movie = Movie(
                    title=row['primaryTitle'].strip(),
                    type=row['titleType'],
                    released_year=start_year,
                    imdb_rating=imdb_rating,
                    netflix_id=None,
                    cancelled=False
                )
            
            movies_to_insert.append(movie)

            categories_raw = row.get('genres', '')
            categories = [cat.strip() for cat in categories_raw.split(',') if cat.strip()]
            for category in categories:
                if category not in category_mapping:
                    category_obj = Category(name=category)
                    categories_to_insert.append(category_obj)
                    category_mapping[category] = category_obj
                movie.categories.append(category_mapping[category])
        

        session.bulk_save_objects(categories_to_insert, return_defaults=False)
        session.bulk_save_objects(movies_to_insert, return_defaults=False)
        session.commit()
        print(f"Chunk {chunk_number} processed in {time.time() - chunk_start_time:.2f} seconds.")

def load_unmatched_netflix_titles(session, netflix_originals, category_mapping):
    all_imdb_titles = pd.read_csv(imdb_titles_file_path)
    movies_to_insert = []
    categories_to_insert = []
    missing_titles_start_time = time.time()
    missing_titles = netflix_originals[~netflix_originals['title_cleaned'].isin(all_imdb_titles['primaryTitle_cleaned'])]

    for _, row in missing_titles.iterrows():
        movie = Movie(
            title=row['title'],
            type=row['type'],
            released_year=int(row['titlereleased']),
            netflix_id=int(row['netflixid']) if 'netflixid' in row else None,
            cancelled=True
        )

        categories_raw = row.get('category', '') 
        categories = [cat.strip() for cat in categories_raw.split(',') if cat.strip()]
        for category in categories:
            if category not in category_mapping:
                category_obj = Category(name=category)
                categories_to_insert.append(category_obj)
                category_mapping[category] = category_obj
            movie.categories.append(category_mapping[category])
        movies_to_insert.append(movie)

    session.bulk_save_objects(categories_to_insert, return_defaults=False)
    session.bulk_save_objects(movies_to_insert, return_defaults=False)
    session.commit()
    print(f"Missing titles processed in {time.time() - missing_titles_start_time:.2f} seconds.")

# +
def load_data():
    Base.metadata.create_all(engine)
    session = Session()

    clear_data(session)
    load_stock_data(session)
    
    try:

        netflix_originals = pd.read_json(netflix_originals_file_path)
        category_mapping = {}
        load_imdb_data(session, netflix_originals, category_mapping)
        load_unmatched_netflix_titles(session, netflix_originals, category_mapping)

    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()
# -


if __name__ == "__main__":
    start_time = time.time()
    load_data()
    end_time = time.time()
    print(f"Time taken to upload data: {end_time - start_time:.2f} seconds")
