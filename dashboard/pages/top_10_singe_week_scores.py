from pathlib import Path

import streamlit as st
from google.cloud import bigquery

GRANDPARENT_DIR = Path(__file__).parents[1]

gbq_client = bigquery.Client()


@st.cache(ttl=60*60*6)
def query_top_10_single_week_scores():
    p = Path(GRANDPARENT_DIR, 'sql', 'top_10_single_week_scores.sql')
    sql = p.read_text()
    df = gbq_client.query(sql).result().to_dataframe()
    return df


def format_top_10_single_week_scores(df):
    df.columns = [
        'Manager',
        'Points',
        "Year",
        "Week",
    ]
    df = df.set_index(['Manager', 'Year', 'Week'])
    df.index.name = ['Manager', 'Year', 'Week']
    df = df.style.set_table_styles(
        [
            {
                'selector': 'td',
                'props': [
                    ('text-align', 'center'),
                ]
            }
        ]
        +
        [
            {
                'selector': 'th',
                'props': [
                    ('text-align', 'center'),
                ]
            }
        ]
    )
    return df


def get_top_10_single_week_scores():
    df = query_top_10_single_week_scores()
    df = format_top_10_single_week_scores(df)
    return df
