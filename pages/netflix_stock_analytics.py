import dash
from dash import html, dcc, callback, Input, Output
import dash_mantine_components as dmc
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import tensorflow as tf
from keras.api.models import Sequential
from keras.api.layers import Dense, LSTM
from sklearn.preprocessing import MinMaxScaler

from pages.queries import get_stock_prices, get_netflix_releases

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/movies_dev_db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

dash.register_page(__name__)

layout = html.Div([
    html.H1('Stock Prices Analytics'),
    dmc.Button('Update Charts', id='update-button', style={'margin': '10px'}),
    dmc.Grid([
        dmc.GridCol([dcc.Graph(id='line-chart')], span=6),
        dmc.GridCol([dcc.Graph(id='scatter-chart')], span=6),
        dmc.GridCol([dcc.Graph(id='prediction-chart')], span=12),
    ], gutter="lg"),
])

def match_release_to_price(release_date, stock_prices):
    closest_date = stock_prices.loc[
        (stock_prices["Trade Time"] - release_date).abs().idxmin()
    ]
    return closest_date["Close"]

def apply_vertical_offset(releases_df):
    offset_increment = 5
    grouped = releases_df.groupby("Release Date")
    for _, group in grouped:
        if len(group) > 1:
            offsets = range(0, len(group) * offset_increment, offset_increment)
            releases_df.loc[group.index, "Adjusted Stock Price"] = group["Stock Price"] + list(offsets)
        else:
            releases_df.loc[group.index, "Adjusted Stock Price"] = group["Stock Price"]
    return releases_df

def normalize_data(data):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data)
    return scaled_data, scaler

def create_dataset(data, time_step=1):
    X, Y = [], []
    for i in range(len(data) - time_step - 1):
        a = data[i:(i + time_step), 0]
        X.append(a)
        Y.append(data[i + time_step, 0])
    return np.array(X), np.array(Y)

def build_lstm_model(time_step):
    model = Sequential()
    model.add(LSTM(50, return_sequences=True, input_shape=(time_step, 1)))
    model.add(LSTM(50, return_sequences=False))
    model.add(Dense(25))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

def train_lstm_model(model, X_train, y_train):
    model.fit(X_train, y_train, batch_size=1, epochs=1)
    return model

def make_predictions(model, X_train, X_test, scaler):
    train_predict = model.predict(X_train)
    test_predict = model.predict(X_test)
    train_predict = scaler.inverse_transform(train_predict)
    test_predict = scaler.inverse_transform(test_predict)
    return train_predict, test_predict

def forecast_future_prices(model, data, time_step, forecast_period, scaler):
    future_predictions = []
    last_data = data[-time_step:]

    for _ in range(forecast_period):
        last_data_scaled = scaler.transform(last_data.reshape(-1, 1))
        X_input = last_data_scaled.reshape(1, time_step, 1)
        next_pred = model.predict(X_input)
        next_pred_rescaled = scaler.inverse_transform(next_pred)
        future_predictions.append(next_pred_rescaled[0, 0])
        last_data = np.append(last_data[1:], next_pred_rescaled)

    return future_predictions

def create_predictions_chart(scaled_data, train_predict, test_predict, future_predictions, time_step, scaler):
    train_predict_plot = np.empty_like(scaled_data)
    train_predict_plot[:, :] = np.nan
    train_predict_plot[time_step:len(train_predict) + time_step, :] = train_predict

    test_predict_plot = np.empty_like(scaled_data)
    test_predict_plot[:, :] = np.nan
    test_predict_plot[len(train_predict) + (time_step * 2) + 1:len(scaled_data) - 1, :] = test_predict

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=np.arange(len(scaled_data)),
        y=scaler.inverse_transform(scaled_data).flatten(),
        mode='lines',
        name='Original Data'
    ))

    fig.add_trace(go.Scatter(
        x=np.arange(len(train_predict_plot)),
        y=train_predict_plot.flatten(),
        mode='lines',
        name='Train Prediction'
    ))

    fig.add_trace(go.Scatter(
        x=np.arange(len(test_predict_plot)),
        y=test_predict_plot.flatten(),
        mode='lines',
        name='Test Prediction'
    ))

    future_predict_plot = np.empty_like(scaled_data)
    future_predict_plot[:, :] = np.nan
    future_predict_plot = np.append(future_predict_plot, future_predictions)

    fig.add_trace(go.Scatter(
        x=np.arange(len(scaled_data), len(scaled_data) + len(future_predictions)),
        y=future_predict_plot[len(scaled_data):],
        mode='lines',
        name='Future Prediction'
    ))

    # Update layout
    fig.update_layout(
        title="Stock Price Predictions",
        xaxis_title="Time",
        yaxis_title="Stock Price"
    )

    return fig

@callback(
    Output('line-chart', 'figure'),
    Output('scatter-chart', 'figure'),
    Output('prediction-chart', 'figure'),
    Input('update-button', 'n_clicks')
)
def update_charts(n_clicks):
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

    # Prepare data for prediction chart
    df = stock_prices_df.set_index('Trade Time')
    data = df[['Close']].values
    scaled_data, scaler = normalize_data(data)
    train_size = int(len(scaled_data) * 0.8)
    train_data = scaled_data[:train_size]
    test_data = scaled_data[train_size:]

    time_step = 60
    X_train, y_train = create_dataset(train_data, time_step)
    X_test, y_test = create_dataset(test_data, time_step)

    X_train = X_train.reshape(X_train.shape[0], X_train.shape[1], 1)
    X_test = X_test.reshape(X_test.shape[0], X_test.shape[1], 1)

    model = build_lstm_model(time_step)
    model = train_lstm_model(model, X_train, y_train)

    train_predict, test_predict = make_predictions(model, X_train, X_test, scaler)

    forecast_period = 252
    future_predictions = forecast_future_prices(model, data, time_step, forecast_period, scaler)

    prediction_chart = create_predictions_chart(scaled_data, train_predict, test_predict, future_predictions, time_step, scaler)

    return line_chart, scatter_chart_netflix_releases, prediction_chart