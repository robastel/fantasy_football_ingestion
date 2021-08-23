from pathlib import Path

import streamlit as st
from google.cloud import bigquery

from pages.all_time_standings import get_all_time_standings
from pages.top_10_singe_week_scores import get_top_10_single_week_scores

PARENT_DIR = Path(__file__).parent
PAGE_TITLE = 'TCCC Fantasy Football \U0001F3C8'
CHARTS = {
    'Welcome': {},
    'All Time Standings': {
        'func': get_all_time_standings,
        'type': st.table,
        'args': [],
        'kwargs': {},
    },
    'Top 10 Single Week Scores': {
        'func': get_top_10_single_week_scores,
        'type': st.table,
        'args': [],
        'kwargs': {},
    }
}


def run():
    st.set_page_config(
        page_title=PAGE_TITLE,
        layout='centered',
        initial_sidebar_state='expanded',
    )
    st.title(PAGE_TITLE)
    chart_name = st.sidebar.radio("Choose a page:", CHARTS, 0)
    if chart_name == 'Welcome':
        st.write('Welcome to the statistics page of TCCC Fantasy Football.'
                 '\n\nChoose a page in the sidebar!')
    else:
        st.header(chart_name)
        chart = CHARTS[chart_name]
        get_results = chart['func']
        results = get_results()
        chart['type'](results, *chart['args'], **chart['kwargs'])


if __name__ == "__main__":
    run()
