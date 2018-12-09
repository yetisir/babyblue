import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

def spawn(plots):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

    app.layout = html.Div([
        dcc.Graph(
            id='life-exp-vs-gdp',
            figure={
                'data': plots,
                'layout': go.Layout(
                    xaxis={'title': 'DateTime'},
                    legend={'x': 0, 'y': 1},
                    hovermode='closest'
                )
            }
        )
    ])

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import itertools

app = dash.Dash()
app.layout = html.Div([
    dcc.Dropdown(
        id='datasource-1',
        options=[
            {'label': i, 'value': i} for i in ['A', 'B', 'C']
        ],
    ),
    dcc.Dropdown(
        id='datasource-2',
        options=[
            {'label': i, 'value': i} for i in ['X', 'Y', 'Z']
        ]
    ),
    html.Hr(),
    html.Div('Dynamic Controls'),
    html.Div(
        id='controls-container'
    ),
    html.Hr(),
    html.Div('Output'),
    html.Div(
        id='output-container'
    )
])

    return app
