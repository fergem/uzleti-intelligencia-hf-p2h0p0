import datetime
import dash
from dash import html, dcc, callback, Input, Output
import dash_mantine_components as dmc
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


from pages.queries import get_netflix_stock_prices, get_stock_prices, get_netflix_releases


DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/movies_dev_db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

dash.register_page(__name__)

# Layout for the stock prices dashboard
layout = html.Div([
    html.H1('Stock Prices Analytics'),
    dmc.Button('Update Charts', id='update-button', style={'margin': '10px'}),
    dmc.Grid([
        dmc.GridCol([dcc.Graph(id='line-chart')], span=6),
        dmc.GridCol([dcc.Graph(id='scatter-chart')], span=6),
        dmc.GridCol([dcc.Graph(id='netflix-stock-prediction')], span=12),
    ], gutter="lg"),
])

def match_release_to_price(release_date, stock_prices):
    # Find the closest matching stock price date
    closest_date = stock_prices.loc[
        (stock_prices["Trade Time"] - release_date).abs().idxmin()
    ]
    return closest_date["Close"]

def apply_vertical_offset(releases_df):
    offset_increment = 5  # Define the vertical offset increment
    grouped = releases_df.groupby("Release Date")
    for _, group in grouped:
        if len(group) > 1:  # Check for overlaps
            offsets = range(0, len(group) * offset_increment, offset_increment)
            releases_df.loc[group.index, "Adjusted Stock Price"] = group["Stock Price"] + list(offsets)
        else:
            releases_df.loc[group.index, "Adjusted Stock Price"] = group["Stock Price"]
    return releases_df

def train_stock_price_model(df):
    # Prepare the features (X) and target (y)
    X = df[['Trade Time Ordinal']]  # Use the ordinal time as the feature
    y = df['Close']  # Target is the 'Close' price

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # Initialize and train the model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Evaluate the model (optional)
    score = model.score(X_test, y_test)
    print(f"Model R^2 score: {score}")

    return model

def predict_future_stock_prices(model, df, days_to_predict=730):
    # Get the last date from the dataset
    last_date = df.index[-1]
    last_ordinal = df['Trade Time Ordinal'].iloc[-1]

    # Generate future dates (ordinal)
    future_dates = [last_ordinal + i for i in range(1, days_to_predict + 1)]
    future_dates = np.array(future_dates).reshape(-1, 1)

    # Make predictions
    future_predictions = model.predict(future_dates)

    # Convert future ordinal dates back to datetime
    future_dates_datetime = [datetime.date.fromordinal(int(i)) for i in future_dates.flatten()]

    # Return the predicted data
    return pd.DataFrame({
        'Date': future_dates_datetime,
        'Predicted Close': future_predictions
    })

@callback(
    Output('line-chart', 'figure'),
    Output('scatter-chart', 'figure'),
    Output("netflix-stock-prediction", "figure"),
    Input('update-button', 'n_clicks')
)
def update_charts(_):
    session = Session()

    stock_prices_df = get_stock_prices(session, "NFLX")
    stock_prices_df["Trade Time"] = pd.to_datetime(stock_prices_df["Trade Time"])

   

    netflix_releases_df = get_netflix_releases(session)
    netflix_releases_df["Release Date"] = pd.to_datetime(netflix_releases_df["Released Year"], format='%Y')
    netflix_releases_df["Stock Price"] = netflix_releases_df["Release Date"].apply(
        lambda date: match_release_to_price(date, stock_prices_df)
    )
    netflix_releases_df = apply_vertical_offset(netflix_releases_df)

    line_chart = px.line(
        stock_prices_df,
        x="Trade Time",
        y="Close",
        title="Stock Price Over Time",
        labels={"Trade Time": "Time", "Close": "Stock Price"}
    )

    scatter_chart_netflix_releases = go.Figure()
    scatter_chart_netflix_releases.add_trace(
    go.Scatter(
        x=stock_prices_df["Trade Time"],
        y=stock_prices_df["Close"],
        mode='lines',
        name='Stock Price'
        )
    )

    scatter_chart_netflix_releases.add_trace(
        go.Scatter(
            x=netflix_releases_df["Release Date"],
            y=netflix_releases_df["Adjusted Stock Price"],
            mode='markers+text',
            text=netflix_releases_df["Title"],
            textposition="top center",  # Positions text above markers
            marker=dict(size=8, color='red'),  # Custom marker styling
            name='Netflix Releases'
        )
    )

    # Update layout
    scatter_chart_netflix_releases.update_layout(
        title="Netflix Releases Impact on Stock Prices",
        xaxis_title="Time",
        yaxis_title="Stock Price"
    )

    stock_prices_df['Trade Time Ordinal'] = stock_prices_df['Trade Time'].apply(lambda x: x.toordinal())  # Convert to ordinal
    stock_prices_df.set_index('Trade Time', inplace=True) 
    model = train_stock_price_model(stock_prices_df)
    forecast = predict_future_stock_prices(model, stock_prices_df)

    scatter_chart_netflix_releases_with_prediction = go.Figure()

    scatter_chart_netflix_releases_with_prediction.add_trace(
        go.Scatter(
            x=stock_prices_df.index,
            y=stock_prices_df['Close'],
            mode='lines',
            name='Historical Stock Price'
        )
    )

    # Add predicted stock prices as a dashed line
    scatter_chart_netflix_releases_with_prediction.add_trace(
        go.Scatter(
            x=forecast['Date'],
            y=forecast['Predicted Close'],
            mode='lines',
            name='Predicted Stock Price',
            line=dict(dash='dash')
        )
    )


    return line_chart, scatter_chart_netflix_releases, scatter_chart_netflix_releases_with_prediction
