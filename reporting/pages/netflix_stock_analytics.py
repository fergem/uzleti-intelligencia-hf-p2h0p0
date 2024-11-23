import dash
from dash import html

dash.register_page(__name__)

layout = html.Div([
    html.H1('Netflix Stock'),
    html.Div('This is our Home page content.'),
])