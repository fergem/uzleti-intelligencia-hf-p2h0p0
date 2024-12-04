import dash
from dash import html
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import dash_mantine_components as dmc
from dash import dcc, callback, Input, Output
import plotly.express as px
import plotly.graph_objs as go

from pages.queries import get_avg_ratings_by_genre, get_categories_by_runtime, get_genre_distribution, get_movie_avg_rating_by_category, get_movies_with_imdb_rating, get_netflix_stock_prices, get_trending_genres_over_time, get_categories_by_runtime

dash.register_page(__name__)

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/movies_dev_db"

layout = html.Div([
    html.H1("IMDb Analytics"),
    dmc.Group([
        dmc.Button("Update Charts", id="update-button", style={"margin": "10px"}),
        dmc.Tabs(
            [
                dmc.TabsList(
                    [
                        dmc.TabsTab("All", value="all"),
                        dmc.TabsTab("Movies", value="movies"),
                        dmc.TabsTab("TV Shows", value="tv-shows"),
                    ]
                ),
            ],
            id="tabs",
            value="all",
        ),
         dmc.Checkbox(
                id="checkbox-state", label="Show only Netflix", checked=False, mb=10
        ),],
        align="center",
        justify="space-between",
    ),
    html.Div(id="charts"),
])



@callback(
    Output("charts", "children"),
    Input("update-button", "n_clicks"),
    Input("checkbox-state", "checked"),
    Input("tabs", "value"),
)
def update_charts(_, show_only_netflix, selected_tab):
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    bar_data = get_movie_avg_rating_by_category(session, show_only_netflix, selected_tab)
    bar_chart = px.bar(
        bar_data,
        x="Genre",
        y="Average IMDb Rating",
        title="Average rating by genre",
        labels={"Average IMDb Rating": "Avg IMDb Rating"}
    )

    pie_data = get_genre_distribution(session, show_only_netflix, selected_tab)
    pie_chart = px.pie(
        pie_data,
        names="Genre",
        values="Movie Count",
        title="Genre Distribution for Netflix Originals"
    )
    pie_chart.update_traces(textposition='inside', textinfo='percent+label')
    
    trending_genres_over_time_data = get_trending_genres_over_time(session, show_only_netflix, selected_tab)
    trending_genres_over_time_data_chart = px.area(trending_genres_over_time_data, 
              x="year", 
              y="movie_count", 
              color="category", 
              title="Trending Categories Over Time", 
              labels={"year": "Release Year", "movie_count": "Number of Movies"})
    trending_genres_over_time_data_chart.update_xaxes(range=[1980, trending_genres_over_time_data["year"].max()-1])

    ratings_data = get_movies_with_imdb_rating(session, show_only_netflix, selected_tab)
    ratings_distribution_chart = px.histogram(ratings_data, x="Rating", nbins=20, title="IMDb Rating Distribution")

    categories_by_runtime_data = get_categories_by_runtime(session, show_only_netflix, selected_tab)
    categories_by_runtime_chart = px.bar(categories_by_runtime_data, x="Genre", y="Average Runtime (Minutes)", title="Genres by average Runtime")

    netflix_avg_rating_by_category = get_avg_ratings_by_genre(session, selected_tab)
    active_trace = go.Bar(
        x=netflix_avg_rating_by_category["Genre"],
        y=netflix_avg_rating_by_category["Avg IMDb Rating (Active)"],
        name="Active",
        marker=dict(color="green")
    )
    cancelled_trace = go.Bar(
        x=netflix_avg_rating_by_category["Genre"],
        y=netflix_avg_rating_by_category["Avg IMDb Rating (Cancelled)"],
        name="Cancelled",
        marker=dict(color="red")
    )

    netflix_avg_rating_by_category_chart = go.Figure(data=[active_trace, cancelled_trace])
    netflix_avg_rating_by_category_chart.update_layout(
        title="Average IMDb Ratings by Genre (Netflix)",
        xaxis_title="Genre",
        yaxis_title="Average IMDb Rating",
        barmode="group",
        legend_title="Status",
    )

    return dmc.Grid([
        dmc.GridCol([dcc.Graph(figure=bar_chart)], span=6),
        dmc.GridCol([dcc.Graph(figure=pie_chart)], span=6),
        dmc.GridCol([dcc.Graph(figure=categories_by_runtime_chart)], span=6),
        dmc.GridCol([dcc.Graph(figure=trending_genres_over_time_data_chart)], span=6),
        dmc.GridCol([dcc.Graph(figure=netflix_avg_rating_by_category_chart)], span=6) if show_only_netflix else None,
        dmc.GridCol([dcc.Graph(figure=ratings_distribution_chart)], span=6),
    ], gutter="lg"),
