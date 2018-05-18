# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from app import app
from app import varlist, callbacks

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
    html.H1(children="Gene Miner",
            style={
                'textAlign': 'center',
                'fontFamily': 'Sans-Serif'}
            ),
    html.H3(children="An integrated cancer data hub",
            style={
                'textAlign': 'center',
                'fontFamily': 'Sans-Serif'}
            ),

    # Cancer type selection
    html.Div([
        html.Label('Choose a type of cancer:'),
        dcc.Dropdown(
            id='cancertype_dropdown',
            options=varlist.cancertypeList,
            value=''
        )
    ], style={'width': '48%', 'display': 'inline-block', 'fontFamily': 'Sans-Serif'}),

    html.Br(),
    html.Br(),
    # Analysis selection
    html.Div([
        dcc.RadioItems(
            id='analysis_radioitems',
            value=''
        )
    ], style={'fontFamily': 'Sans-Serif'}),

    # Analysis Board
    html.Hr(),
    html.Div(id='left',
             style={'width': '53%', 'fontFamily': 'Sans-Serif',
                    'position': 'absolute',
                    'bottom': '20px', 'left': '0px',
                    }
             ),
    html.Div(id='right-top',
             style={'width': '46%', #'height': '48%',
                    'float': 'right', 'position': 'absolute',
                    'bottom': '280px', 'right': '0px',
                    'fontFamily': 'Sans-Serif',}
             ),
    html.Div(id='right-bottom',
             style={'width': '46%', #'height': '48%',
                    'float': 'right', 'position': 'absolute',
                    'bottom': '20px', 'right': '0px',
                    'fontFamily': 'Sans-Serif',}
             ),
])

# Refine analysis tab menu from cancer type
@dashapp.callback(
    Output('analysis_radioitems', 'options'),
    [Input('cancertype_dropdown', 'value')])
def update_analysis_tabs(cancertype):
    return varlist.dropdownDict[cancertype]


# Run analysis
@dashapp.callback(
    Output('right-top', 'children'),
    [Input('cancertype_dropdown', 'value'),
     Input('analysis_radioitems', 'value'),])
def plot_analysis_2(cancertype, analysistype):
    if analysistype=='clinical':
        return callbacks.right_top_clinical(cancertype)
    elif analysistype=='geneexpr':
        return callbacks.right_top_geneexpr(cancertype)
    else:
        return ''

@dashapp.callback(
    Output('right-bottom', 'children'),
    [Input('cancertype_dropdown', 'value'),
     Input('analysis_radioitems', 'value'),])
def plot_analysis(cancertype, analysistype):
    if analysistype=='clinical':
        return callbacks.right_bottom_clinical(cancertype)
    elif analysistype=='geneexpr':
        return callbacks.right_bottom_geneexpr(cancertype)
    else:
        return ''

@dashapp.callback(
    Output('left', 'children'),
    [Input('cancertype_dropdown', 'value'),
     Input('analysis_radioitems', 'value'),])
def plot_analysis(cancertype, analysistype):
    if analysistype=='clinical':
        return callbacks.left_clinical(cancertype)
    elif analysistype=='geneexpr':
        return callbacks.left_geneexpr(cancertype)
    elif analysistype == '' or analysistype == None:
        return ''
    else:
        return callbacks.undef()