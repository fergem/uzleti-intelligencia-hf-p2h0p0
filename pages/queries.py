

import pandas as pd
from sqlalchemy import desc, func
from db_schema import Category, Company, Movie, StockPrice, movie_categories

def apply_selected_tab_filter(query, selected_tab):
    if selected_tab == "movies":
        query = query.filter(Movie.type == "movie")
    elif selected_tab == "tv-shows":
        query = query.filter(Movie.type == "tvSeries")
    return query

def get_movie_avg_rating_by_category(session, show_only_netflix, selected_tab):
    query = session.query(
            Category.name,
            func.avg(Movie.imdb_rating).label("avg_rating")
        ).join(movie_categories, movie_categories.c.category_id == Category.id)\
        .join(Movie, Movie.id == movie_categories.c.movie_id)\
        .filter(Movie.imdb_rating.isnot(None))\
        .filter(Movie.netflix_id.isnot(None) if show_only_netflix else True)
    
    query = apply_selected_tab_filter(query, selected_tab)

    result = query.group_by(Category.name)\
                  .order_by(desc("avg_rating"))\
                  .all()

    return pd.DataFrame(result, columns=["Genre", "Average IMDb Rating"])

def get_trending_genres_over_time(session, show_only_netflix, selected_tab):
    query = session.query(
        Movie.released_year,
        Category.name,
        func.count(Movie.id).label("movie_count")
    ).filter(
        Movie.netflix_id.isnot(None) if show_only_netflix else True).join(
        movie_categories, movie_categories.c.movie_id == Movie.id
    ).join(
        Category, Category.id == movie_categories.c.category_id
    )

    query = apply_selected_tab_filter(query, selected_tab)

    result = query.group_by(
        Movie.released_year,
        Category.name
    ).order_by(
        Movie.released_year,
        func.count(Movie.id).desc()
    ).all()

    data = [{
        "year": item.released_year,
        "category": item.name,
        "movie_count": item.movie_count
    } for item in result]

    df = pd.DataFrame(data)
    return df

def get_categories_by_runtime(session, show_only_netflix, selected_tab):
    query = session.query(
        Category.name,
        func.avg(Movie.runtime_minutes).label("avg_runtime")
    ).join(movie_categories, movie_categories.c.category_id == Category.id)\
        .join(Movie, movie_categories.c.movie_id == Movie.id)\
        .filter(Movie.runtime_minutes.isnot(None))\
        .filter(Movie.netflix_id.isnot(None) if show_only_netflix else True)
    
    query = apply_selected_tab_filter(query, selected_tab)

    result = query.group_by(Category.name)\
                  .order_by(desc("avg_runtime"))\
                  .all()

    return pd.DataFrame(result, columns=["Genre", "Average Runtime (Minutes)"])

def get_genre_distribution(session, show_only_netflix, selected_tab):
    query = session.query(
        Category.name,
        func.count(Movie.id).label("movie_count")
    ).join(movie_categories, movie_categories.c.category_id == Category.id)\
        .join(Movie, movie_categories.c.movie_id == Movie.id)\
        .filter(Movie.netflix_id.isnot(None) if show_only_netflix else True)
    
    query = apply_selected_tab_filter(query, selected_tab)

    result = query.group_by(Category.name)\
                  .all()

    return pd.DataFrame(result, columns=["Genre", "Movie Count"])

def get_movies_with_imdb_rating(session, show_only_netflix, selected_tab):
    query = session.query(Movie.imdb_rating).filter(Movie.imdb_rating.isnot(None))\
        .filter(Movie.netflix_id.isnot(None) if show_only_netflix else True)
    
    query = apply_selected_tab_filter(query, selected_tab)

    ratings = query.all()
    return pd.DataFrame(ratings, columns=["Rating"])

def get_avg_ratings_by_genre(session, selected_tab):
    query = session.query(
        Category.name.label("genre"),
        func.avg(func.nullif(Movie.imdb_rating, 0)).filter(Movie.cancelled == False).label("avg_rating_active"),
        func.avg(func.nullif(Movie.imdb_rating, 0)).filter(Movie.cancelled == True).label("avg_rating_cancelled"),
    ).join(movie_categories, movie_categories.c.category_id == Category.id)\
     .join(Movie, movie_categories.c.movie_id == Movie.id)\
     .filter(Movie.imdb_rating.isnot(None))\
     .filter(Movie.netflix_id.isnot(None))\
     .group_by(Category.name)\
     .order_by(Category.name)
    
    query = apply_selected_tab_filter(query, selected_tab)

    result = query.all()
    return pd.DataFrame(result, columns=["Genre", "Avg IMDb Rating (Active)", "Avg IMDb Rating (Cancelled)"])

## FOR STOCKS

def get_stock_prices(session, company_symbol):
    stock_data = session.query(
        StockPrice.trade_time, StockPrice.close, StockPrice.volume
    ).join(Company).filter(Company.symbol == company_symbol).order_by(StockPrice.trade_time).all()

    return pd.DataFrame(stock_data, columns=["Trade Time", "Close", "Volume"])

def get_netflix_stock_prices(session):
    stock_data = (
        session.query(StockPrice.trade_time, StockPrice.close, StockPrice.volume, StockPrice.high, StockPrice.low, StockPrice.open)
        .join(Company, StockPrice.company_id == Company.id)
        .filter(Company.symbol == 'NFLX')
    ).all()

    df = pd.DataFrame(stock_data, columns=["Trade Time", "Close", "Volume", "High", "Low", "Open"])
    df['Trade Time'] = pd.to_datetime(df['Trade Time'])
    return df


def get_netflix_releases(session):
    netflix_releases = session.query(
        Movie.released_year, Movie.title
    ).filter(Movie.netflix_id.isnot(None)).filter(Movie.released_year > 0).filter(Movie.imdb_rating > 8.6).filter(Movie.released_year > 2015).all()

    return pd.DataFrame(netflix_releases, columns=["Released Year", "Title"])