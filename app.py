import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import requests
import pandas as pd

from datetime import datetime
from flask_caching import Cache
from pathlib import Path

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache'
})
app.config.suppress_callback_exceptions = True


data = [
    {
        'id': 'datasets',
        'url': 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3',
        'date_cols': ['created_at', 'last_modified'],
    },
    {
        'id': 'resources',
        'url': 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d',
        'date_cols': ['created_at', 'modified'],
    },
    {
        'id': 'reuses',
        'url': 'https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379',
        'date_cols': ['created_at', 'last_modified'],
    },
    {
        'id': 'discussions',
        'url': 'https://www.data.gouv.fr/fr/datasets/r/d77705e1-4ecd-461c-8c24-662d47c4c2f9',
        'date_cols': ['created', 'closed'],
    },
    {
        'id': 'organisations',
        'url': 'https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7',
        'date_cols': ['created_at', 'last_modified'],
    },    
]


def download_has_changed():
    has_changed = False
    data_path = Path('./data')
    data_path.mkdir(exist_ok=True)
    for item in data:
        r = requests.head(item['url'])
        location = r.headers['Location']
        filename = location.split('/')[-1]
        filepath = data_path / filename
        if not filepath.exists():
            has_changed = True
            r = requests.get(location)
            with open(filepath, 'wb') as dfile:
                dfile.write(r.content)
        item['filepath'] = filepath
    return has_changed


@cache.cached(unless=download_has_changed or app.config.DEBUG)
def do_update_data():
    # load CSVs into dataframes
    for datum in data:
        datum['df'] = pd.read_csv(datum['filepath'], delimiter=';', parse_dates=datum['date_cols'])

    # compute object creation by year
    df_year = None
    df_year_no_geo = None
    for datum in data:
        created = datum['date_cols'][0]
        _df = datum['df'].groupby(pd.Grouper(key=created, freq="Y")).count()['id'].rename(datum['id'])
        if datum['id'] == 'datasets':
            _df_no_geo = datum['df'][~datum['df']['description'].str.contains('geo.data.gouv.fr', na=False)].groupby(pd.Grouper(key=created, freq="Y")).count()['id'].rename(datum['id'])
        elif datum['id'] == 'resources':
            _df_no_geo = datum['df'][~datum['df']['url'].str.contains('files.geo.data.gouv.fr', na=False)].groupby(pd.Grouper(key=created, freq="Y")).count()['id'].rename(datum['id'])
        else:
            _df_no_geo = _df
        df_year = _df if df_year is None else pd.merge(df_year, _df, right_index=True, left_index=True)
        df_year_no_geo = _df_no_geo if df_year_no_geo is None else pd.merge(df_year_no_geo, _df_no_geo, right_index=True, left_index=True)
    fig_year = px.bar(
        df_year, x=df_year.index.year, y=[d['id'] for d in data], 
        title='Nombre d\'objets créés par an'
    )
    fig_year_no_geo = px.bar(
        df_year_no_geo, x=df_year_no_geo.index.year, y=[d['id'] for d in data], 
        title='Nombre d\'objets créés par an'
    )

    # compute object creation by month
    df_month = None
    df_month_no_geo = None
    start_date = datetime.now() - pd.Timedelta(days=365)
    start_date = start_date.replace(day=1, hour=0, minute=0, second=0)
    for datum in data:
        created = datum['date_cols'][0]
        _df = datum['df'][datum['df'][created] >= start_date].groupby(pd.Grouper(key=created, freq="M")).count()['id'].rename(datum['id'])
        if datum['id'] == 'datasets':
            _df_no_geo = datum['df'][~datum['df']['description'].str.contains('geo.data.gouv.fr', na=False)][datum['df'][created] >= start_date].groupby(pd.Grouper(key=created, freq="M")).count()['id'].rename(datum['id'])
        elif datum['id'] == 'resources':
            _df_no_geo = datum['df'][~datum['df']['url'].str.contains('files.geo.data.gouv.fr', na=False)][datum['df'][created] >= start_date].groupby(pd.Grouper(key=created, freq="M")).count()['id'].rename(datum['id'])
        else:
            _df_no_geo = _df
        df_month = _df if df_month is None else pd.merge(df_month, _df, right_index=True, left_index=True)
        df_month_no_geo = _df_no_geo if df_month_no_geo is None else pd.merge(df_month_no_geo, _df_no_geo, right_index=True, left_index=True)
    fig_month = px.bar(
        df_month, x=df_month.index.strftime("%Y-%m"), y=[d['id'] for d in data], 
        title='Nombre d\'objets créés dans les 12 derniers mois'
    )
    fig_month_no_geo = px.bar(
        df_month_no_geo, x=df_month_no_geo.index.strftime("%Y-%m"), y=[d['id'] for d in data], 
        title='Nombre d\'objets créés dans les 12 derniers mois'
    )

    return fig_year, fig_month, fig_year_no_geo, fig_month_no_geo


def serve_layout():
    fig_year, fig_month, fig_year_no_geo, fig_month_no_geo = do_update_data()
    return html.Div(children=[
        html.H1(children='Données du catalogue data.gouv.fr'),
        html.H2(children='Catalogue complet'),
        dcc.Graph(
            id='year',
            figure=fig_year,
        ),
        dcc.Graph(
            id='month',
            figure=fig_month,
        ),
        html.H2(children='Catalogue hors geo.data.gouv.fr'),
        dcc.Graph(
            id='year_no_geo',
            figure=fig_year_no_geo,
        ),
        dcc.Graph(
            id='month_no_geo',
            figure=fig_month_no_geo,
        ),        
    ])


app.layout = serve_layout

if __name__ == '__main__':
    app.run_server(debug=True)
