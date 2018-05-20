import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas.io.sql as psql
from app import conn, varlist

top10 = None
bottom10 = None
def readdata(cancer):
    fc = psql.read_sql("SELECT * FROM %s_stage2to1" % cancer, conn)
    global top10;
    top10 = fc[:10].copy(deep=True)
    global bottom10;
    bottom10 =fc[-10:].copy(deep=True)[::-1]

def undef():
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
    df = psql.read_sql("SELECT * FROM %s_stage2and1" % cancer, conn)
    # df['annotation'] = df['gene_name'] + '(<br />' + df['info'].apply(lambda x: x.split('[')[0]) + ')'
    return dcc.Graph(
        figure=go.Figure(
            data=[go.Scatter(
                x=df.stage1_avg_expr,
                y=df.stage2_avg_expr,
                text=df.gene_name,
                # textfont={'size': '18'},
                mode='markers',
                marker={
                    'size': 4,
                    'opacity': 0.5,
                    'line': {'width': 0.5, 'color': 'white'}}
                )],
            layout = go.Layout(
                title='Gene Expr: Stage II vs Stage I',
                titlefont={'size': '18'},
                xaxis={
                    'title': 'Stage I gene expr value (log)',
                    'type': 'log' # 'linear'
                },
                yaxis={
                    'title': 'Stage II gene expr value (log)',
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
                width=450,
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
    count = 10
    #df = psql.read_sql("SELECT * FROM %s_stage2to1" % cancer, conn)[:10]
    top10['annotation'] = top10['gene_name'] + '<br />' + top10['info'].apply(lambda x: x.split('[')[0])
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Bar(
                    x=list(range(1, count+1)),
                    y=top10.fold_change,
                    text=top10.annotation,
                    textfont={'size':'16'})
                ],
            layout=go.Layout(
                title='Top %s genes INCREASED: Stage II vs stage I' % count,
                titlefont={'size':'16'},
                height=250,
                width=450,
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
                width=450,
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
    count = 10
    #df = psql.read_sql("SELECT * FROM %s_stage2to1" % cancer, conn)[-10:]
    bottom10['annotation'] = bottom10['gene_name'] + '<br />' + bottom10['info'].apply(lambda x: x.split('[')[0])
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Bar(
                    x=list(range(1, count+1)),
                    y=bottom10.fold_change,
                    text=bottom10.annotation,
                    textfont={'size':'16'})
                ],
            layout=go.Layout(
                title='Top %s genes DECREASED: Stage II vs stage I' % count,
                titlefont={'size':'16'},
                height=250,
                width=450,
                showlegend=False,
                margin=go.Margin(l=60, r=40, t=60, b=40),
                paper_bgcolor=varlist.colors['background'],
                plot_bgcolor=varlist.colors['background'],
                font={'color': varlist.colors['text'], 'size':'12'}
            )
        ),
        id='gene_expr_bar'
    )