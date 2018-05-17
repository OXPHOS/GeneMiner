import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas.io.sql as psql
from app import conn

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

# Filtypes and analysis can be run
ftable = {'label': 'Summary', 'value': 'summary'}
fclinical= {'label': 'Clinical report', 'value': 'clinical'}
fgeneexpr = {'label': 'Gene expression analysis', 'value': 'geneexpr'}
fsnp = {'label': 'SNP analysis', 'value': 'snp'}
fnull = {'':''}

#
cancertypeList = [
    {'label': '', 'value': ''},
    {'label': 'Breast cancer', 'value': 'Breast'},
    {'label': 'Brain tumor', 'value': 'Brain'},
    {'label': 'Lung cancer', 'value': 'Lung'}
]

# Master selection dict
dropdownDict = {
    '':[fnull],
    'Breast': [fnull, ftable, fclinical, fgeneexpr, fsnp],
    'Brain': [fnull, ftable, fclinical, fsnp],
    'Lung': [fnull, ftable, fclinical]
}


def plots_summary(cancer):
    df = psql.read_sql("SELECT * FROM patient_info WHERE disease_type = '%s' " % cancer, conn)
    return html.Div([html.H4(children='Cancer information summary'),
            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +
                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(min(len(df), 100))]
            )
    ], style={'position': 'center','width': '100%','display': 'inline-block', 'padding': '0 20'})


def plots_clinical(cancer):
    df = psql.read_sql("SELECT COUNT(*) AS counts, disease_stage FROM patient_info "
                       "WHERE disease_type = '%s' GROUP BY disease_stage " % cancer, conn)
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Pie(
                    values=df.counts, labels=df.disease_stage)
            ],
            layout = go.Layout(
                title='Stage Summary',
                )
            ),
            style = {'height': 400},
            id = 'stage_pie',
        )


def plots_geneexpr(cancer):
    count = 50
    df = psql.read_sql("SELECT * FROM %s_stage2to1 LIMIT %s" % (cancer, count), conn)
    return dcc.Graph(
        figure=go.Figure(
            data=[
                go.Bar(
                    x=list(range(1, count+1)),
                    y=df.fold_change,
                    name='',
                    marker=go.Marker(
                        color='rgb(55, 83, 109)'))
                ],
            layout=go.Layout(
                title='Top 10 genes changed between Stage II and stage I in Breast cancer',
                showlegend=False,
                legend=go.Legend(
                    x=0,
                    y=1.0
                ),
                margin=go.Margin(l=40, r=0, t=40, b=30)
            )
        ),
        style={'height': 300},
        id='gene_expr_bar'
    )


analysisDict = {
    'summary': plots_summary,
    'clinical': plots_clinical,
    'geneexpr': plots_geneexpr
}
