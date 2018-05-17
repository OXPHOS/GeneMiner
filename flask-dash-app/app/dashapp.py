# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from app import app
from app import varlist

# Test
@app.route('/index')
def sayHi():
    return "Hi from my Flask App!"

# Define for IIS module registration.
wsgi_app = app.wsgi_app

# Connect dash to flask
dashapp = dash.Dash(__name__, server=app, url_base_pathname='/')

# Setup page
dashapp.layout = html.Div(children=[
    # Title of the website
    html.H1(children="Welcome to Gene Miner",
            style={
                'textAlign': 'center'}
            ),

    html.H3(children="An integrated cancer data hub",
            style={
                'textAlign': 'center'}
            ),
    html.Br(),
    html.Br(),

    # Cancer type selection
    html.Div([
        html.Label('Choose a type of cancer:'),
        html.Br(),
        html.Br(),
        dcc.Dropdown(
            id='cancertype_dropdown',
            options=varlist.cancertypeList,
            value='',
        )
    ], style={'width': '48%', 'display': 'inline-block'}),

    # Analysis selection
    html.Div([
        html.Label('Choose a type of analysis:'),
        html.Br(),
        html.Br(),
        dcc.Dropdown(
            id='analysis_dropdown',
            value=''
        )
    ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}),

    # Analysis Board
    html.Br(),
    html.Br(),
    html.Hr(),
    html.Div(id='analysis_board')
])


# Refine analysis dropdown menu from cancer type
@dashapp.callback(
    Output('analysis_dropdown', 'options'),
    [Input('cancertype_dropdown', 'value')])
def update_analysis_dropdown(cancertype):
    return varlist.dropdownDict[cancertype]


# Run analysis
@dashapp.callback(
    Output('analysis_board', 'children'),
    [Input('cancertype_dropdown', 'value'),
     Input('analysis_dropdown', 'value'),])
def plot_analysis(cancertype, analysistype):
    # print(cancertype, analysistype)
    if analysistype in varlist.analysisDict:
        return varlist.analysisDict[analysistype](cancertype)
    elif analysistype == '' or analysistype == None:
        return ''
    else:
        return [
            html.Br(),
            html.Br(),
            html.H1(
                children="Looks like you're in the middle of nowhere ;(",
                style={
                    'textAlign': 'center',
                    'color': '#800000',
                }
            )]
