from pathlib import Path

import streamlit as st
from google.cloud import bigquery

GRANDPARENT_DIR = Path(__file__).parents[1].resolve()

gbq_client = bigquery.Client()


@st.cache(ttl=60*60*6)
def query_all_time_standings():
    p = Path(GRANDPARENT_DIR, 'sql', 'all_time_standings.sql')
    sql = p.read_text()
    df = gbq_client.query(sql).result().to_dataframe()
    return df


def format_all_time_standings(df):
    df.columns = [
        'Manager',
        "\U0001F947",
        "\U0001F948",
        "\U0001F949",
        'Reg. Season Win Rate',
        'Made Playoffs Rate',
        'Seasons Played'
    ]
    df = df.astype(str)
    df = df.replace('0', '')
    df = df.set_index('Manager')
    df = df.style.set_table_styles(
        [
            {
                'selector': f'thead tr th:nth-of-type({n})',
                'props': [
                    ('font-size', '3em'),
                ]
            }
            for n in range(2, 5)
        ]
        +
        [
            {
                'selector': 'td',
                'props': [
                    ('text-align', 'center'),
                ]
            },
            {
                'selector': 'th',
                'props': [
                    ('text-align', 'center'),
                ]
            }
        ]
    )
    return df


def get_all_time_standings():
    df = query_all_time_standings()
    df = format_all_time_standings(df)
    return df
