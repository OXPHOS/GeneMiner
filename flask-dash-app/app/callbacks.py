# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Interactive graph implementation

Author: Pan Deng

"""

import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas.io.sql as psql
from app import conn, varlist


count = 10
top = None
bottom = None
def readdata(cancer):
    """
    Pre-load dataset from database to avoid concurrent reading from one table
    
    :param cancer: type of cancer queried
    """
    fc = psql.read_sql("SELECT * FROM exprsort_%s" % cancer.replace(' ', ''), conn)
    global top;
    top = fc[:count].copy(deep=True)
    global bottom;
    bottom = fc[-count:].copy(deep=True)[::-1]


def undef():
    """
    For undefined behaviors
    """
    return [
        html.Br(),
        html.Br(),
        html.H1(
            children="Looks like you're in the middle of nowhere ;( ",
            style={
                'textAlign': 'center',
                'fontFamily': 'Sans-Serif',
                'color': '#800000',
            }
        )]


def left_clinical(cancer):
    """
    Display patient_info table of queried cancer 
    """
    df = psql.read_sql("SELECT * FROM patient_info WHERE disease_type = '%s' " % cancer, conn)
    return dcc.Graph(
         figure=go.Figure(
             data=[go.Table(
                 header=dict(values=df.columns,
                             fill=dict(color='#B4AF21'),
                             align=['left'] * 5),
                 cells=dict(values=[df.case_id, df.project_id,
                                    df.disease_type, df.disease_stage,
                                    df.gender],
                            fill = dict(color='#464411'),
                            align=['left'] * 5))
             ],
             layout=go.Layout(
                 title='Cancer information table',
                 titlefont={'size': '18'},
                 font={'color': varlist.colors['text']},
                 height=510,
                 margin=go.Margin(
                     l=30,
                     r=30,
                     b=30,
                     t=50,
                     pad=0
                 ),
                 paper_bgcolor = varlist.colors['background'],
             )
         ),
         id='summary_table'
    )


def left_geneexpr(cancer):
    """
    Display gene expression comparison of given cancer type and queried stages as 2D scatter plot. 
    """
    df = psql.read_sql("SELECT * FROM exprcmp_%s" % cancer.replace(' ', ''), conn)
    if not varlist.stages[cancer]:
        return ''
    return dcc.Graph(
        figure=go.Figure(
            data=[go.Scatter(
                x=df.early_avg_expr,
                y=df.late_avg_expr,
                text=df.gene_name,
                # textfont={'size': '18'},
                mode='markers',
                marker={
                    'size': 4,
                    'opacity': 0.5,
                    'line': {'width': 0.5, 'color': 'white'}}
                )],
            layout = go.Layout(
                title='Gene Expr: %s vs %s' % (varlist.stages[cancer][1], varlist.stages[cancer][0]),
                titlefont={'size': '18'},
                xaxis={
                    'title': '%s gene expr value (log)' % varlist.stages[cancer][1],
                    'type': 'log' # 'linear'
                },
                yaxis={
                    'title': '%s gene expr value (log)' % varlist.stages[cancer][0],
                    'type': 'log'
                },
                margin={'l': 70, 'b': 80, 't': 80, 'r': 20},
                height=510,
                hovermode='closest',
                paper_bgcolor=varlist.colors['background'],
                plot_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size': '12'}
            )
        ),
        id='gene_expr_scatter'
    )


def right_top_clinical(cancer):
    """
    Stage summary in pie chart. 
    """
    df = psql.read_sql("SELECT COUNT(*) AS counts, disease_stage FROM patient_info "
                       "WHERE disease_type = '%s' GROUP BY disease_stage " % cancer, conn)
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Pie(
                    values=df.counts,
                    labels=df.disease_stage,
                    textfont={'color': '#060606', 'size':'12'})
            ],
            layout = go.Layout(
                title='Stage Summary',
                titlefont={'size': '18'},
                height=250,
                # width=450,
                margin=go.Margin(
                    l=100,
                    r=100,
                    b=10,
                    t=50,
                    pad=0
                ),
                paper_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size':'12'}
                )
            ),
            id = 'stage_pie',
        )


def right_top_geneexpr(cancer):
    """
    Top 10 genes changed - bar chart. 
    """
    if not varlist.stages[cancer]:
        return ''

    try:
        top['annotation'] = top['gene_name'] + '<br />' + top['info'].apply(lambda x: x.split('[')[0])
    except AttributeError:
        top['annotation'] = top['gene_name'] + '<br />' + top['info']
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Bar(
                    x=list(range(1, count+1)),
                    y=top.fold_change,
                    text=top.annotation,
                    textfont={'size':'16'})
                ],
            layout=go.Layout(
                title='Top %s genes INCREASED: %s vs stage %s'
                      % (count, varlist.stages[cancer][1], varlist.stages[cancer][0]),
                titlefont={'size':'16'},
                height=250,
                # width=450,
                showlegend=False,
                margin=go.Margin(l=40, r=40, t=60, b=40),
                paper_bgcolor=varlist.colors['background'],
                plot_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size':'12'}
            )
        ),
        id='gene_expr_bar'
    )


def right_bottom_clinical(cancer):
    """
    Stage summary in pie chart. 
    """
    df = psql.read_sql("SELECT COUNT(*) AS counts, gender FROM patient_info "
                       "WHERE disease_type = '%s' GROUP BY gender " % cancer, conn)
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Pie(
                    values=df.counts,
                    labels=df.gender,
                    textfont={'color': '#060606', 'size':'12'})
            ],
            layout = go.Layout(
                title='Gender Summary',
                titlefont={'size': '18'},
                height=250,
                # width=450,
                margin=go.Margin(
                    l=100,
                    r=100,
                    b=10,
                    t=50,
                    pad=0
                ),
                paper_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size':'12'}
                )
            ),
            id = 'gender_pie',
        )


def right_bottom_geneexpr(cancer):
    """
    Bottom 10 genes changed - bar chart. 
    """
    if not varlist.stages[cancer]:
        return ''

    try:
        bottom['annotation'] = bottom['gene_name'] + '<br />' + bottom['info'].apply(lambda x: x.split('[')[0])
    except AttributeError:
        bottom['annotation'] = bottom['gene_name'] + '<br />' + bottom['info']
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Bar(
                    x=list(range(1, count+1)),
                    y=bottom.fold_change,
                    text=bottom.annotation,
                    textfont={'size':'16'})
                ],
            layout=go.Layout(
                title='Top %s genes DECREASED: %s vs %s'
                      % (count, varlist.stages[cancer][1], varlist.stages[cancer][0]),
                titlefont={'size':'16'},
                height=250,
                # width=450,
                showlegend=False,
                margin=go.Margin(l=60, r=40, t=60, b=40),
                paper_bgcolor=varlist.colors['background'],
                plot_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size':'12'}
            )
        ),
        id='gene_expr_bar'
    )