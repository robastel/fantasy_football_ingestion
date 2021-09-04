from abc import ABC, abstractmethod
from pathlib import Path

import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

import utils

SQL_DIR = Path(Path(__file__).parent.resolve(), "sql")
GBQ_CLIENT = bigquery.Client()
PAGE_TITLE = "\U0001F3C6 TCCC Fantasy Football \U0001F3C8"


class Charter(ABC):
    def __init__(self, file):
        self.sql_path = Path(SQL_DIR, file)

    @st.cache(ttl=60 * 60 * 6)
    def get_df(self):
        sql = self.sql_path.read_text()
        df = GBQ_CLIENT.query(sql).result().to_dataframe()
        return df

    @abstractmethod
    def get_chart(self, df, *args, **kwargs):
        raise NotImplementedError

    def __call__(self):
        df = self.get_df()
        self.get_chart(df)


class AllTimeStandings(Charter):
    def get_chart(self, df, *args, **kwargs):
        df.columns = [
            "Manager",
            "\U0001F947",
            "\U0001F948",
            "\U0001F949",
            "Made Playoffs Rate",
            "Reg. Season Win Rate",
            "Seasons Played",
        ]
        df = df.astype(str)
        df = df.replace("0", "")
        df = df.set_index("Manager")
        df = df.style.set_table_styles(
            utils.INCREASE_TABLE_FONT_SIZE
            + [
                {
                    "selector": f"thead tr th:nth-of-type({n})",
                    "props": [
                        ("font-size", "3em"),
                    ],
                }
                for n in range(2, 5)
            ]
            + utils.CENTER_ALIGN_TABLE_TEXT
        )
        st.table(df)


class ScoringBoxPlots(Charter):
    def get_chart(self, df, *args, **kwargs):
        manager_medians = (
            df.groupby("manager_initials")["points"].median().sort_values()
        )
        managers = manager_medians.index
        normalized_medians = mcolors.Normalize(
            vmin=manager_medians.min(),
            vmax=manager_medians.max(),
        )
        color_map = plt.cm.get_cmap("RdYlGn")
        fig = go.Figure()
        for manager, median in zip(managers, manager_medians):
            fig.add_trace(
                go.Box(
                    x=df[df["manager_initials"] == manager]["points"],
                    name=manager,
                    orientation=kwargs.get("orientation", "h"),
                    line_color=f"rgba{str(color_map(normalized_medians(median)))}",
                    boxpoints=False,
                    fillcolor="rgba(0, 0, 0, 0)",
                )
            )
        fig.update_layout(
            **utils.PLOTLY_DEFAULT_LAYOUT_KWARGS,
            title_text="All Time Scoring (hover to see exact values)",
            xaxis_title_text="Weekly Points Scored",
            xaxis_showgrid=True,
            xaxis_zeroline=False,
            yaxis_title_text="Manager",
            yaxis_showgrid=False,
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.write(
            "How to read this box-and-whiskers plot:"
            "\n\nEach manager has a boxplot consisting of 5 values."
            "\n - The leftmost tick represents the manager's least number of"
            " points ever scored in a week."
            "\n - The left side of the box represents the manager's 25th"
            " percentile score. Roughly 25% of the manager's all time scores"
            " fall below this score (meaning roughly 75% are greater than this"
            " score)."
            "\n - The line within the box represents the manager's median"
            " score. Roughly 50% of the manager's all time scores fall on"
            " either side of this score."
            "\n - The right side of the box represents the manager's 75th"
            " percentile score. Roughly 75% of the manager's all time scores"
            " fall below this score (meaning roughly 25% are greater than this"
            " score)."
            "\n - The rightmost tick represents the manager's greatest number"
            " of points ever scored in a week."
        )


class DraftPickPlayerPositions(Charter):
    def get_chart(self, df, *args, **kwargs):
        dfc = df.copy()
        dfc = dfc.set_index("manager_initials")
        df_all_managers = dfc.groupby('round_num').sum()
        print(dfc.head())
        print(df_all_managers.head())
        all_managers_name = 'All Managers'
        df_all_managers.index = [all_managers_name]*len(df_all_managers)
        df_combined = pd.concat([dfc, df_all_managers])
        manager = st.selectbox(
            "Choose an option:",
            [all_managers_name] + list(np.sort(dfc.index.unique())),
            0,
        )
        round_nums = np.sort(dfc["round_num"].unique())
        player_positions = dfc.drop("round_num", axis=1).columns
        df_pos = df_combined[player_positions]
        df_pos = 100 * df_pos.div(df_pos.sum(axis=1), axis=0)
        fig = go.Figure()
        for pos in player_positions:
            fig.add_trace(
                go.Bar(
                    x=round_nums,
                    y=df_pos.loc[manager, pos],
                    name=pos.upper(),
                )
            )
        fig.update_layout(
            **utils.PLOTLY_DEFAULT_LAYOUT_KWARGS,
            barmode='relative',
            title_text="Draft Pick Player Position by Round",
            xaxis_title_text='Draft Round',
            xaxis_type='category',
            yaxis_title_text='Share of Draft Picks',
            yaxis_range=[0, 100],
            yaxis_ticksuffix="%",
            legend_title_text='Click positions to filter:',
            legend_traceorder='reversed',
        )
        st.plotly_chart(fig, use_container_width=True)


class TopSingleWeekScores(Charter):
    def get_chart(self, df, *args, **kwargs):
        dfc = df.copy()
        dfc.columns = [
            "Manager",
            "Points",
            "Year",
            "Week",
        ]
        dfc["concat"] = (
            dfc["Manager"] + dfc["Year"] + dfc["Week"]
        )
        dfc = dfc.set_index("concat")
        dfc = pd.DataFrame(dfc["Points"]).style.set_table_styles(
            utils.CENTER_ALIGN_TABLE_TEXT + utils.INCREASE_TABLE_FONT_SIZE
        )
        st.table(dfc)


CHARTS = {
    "Welcome": {},
    "All Time Standings": {
        "class": AllTimeStandings,
        "sql": "all_time_standings.sql",
        "args": [],
        "kwargs": {},
    },
    "Scoring Box Plots": {
        "class": ScoringBoxPlots,
        "sql": "matchups_h2h.sql",
        "args": [],
        "kwargs": {},
    },
    "Draft Pick Player Positions": {
        "class": DraftPickPlayerPositions,
        "sql": "draft_pick_player_positions.sql",
        "args": [],
        "kwargs": {},
    },
    "Top Single Week Scores": {
        "class": TopSingleWeekScores,
        "sql": "top_single_week_scores.sql",
        "args": [],
        "kwargs": {},
    },
}


def run():
    st.set_page_config(
        page_title=PAGE_TITLE,
        layout="centered",
        initial_sidebar_state="expanded",
    )
    st.title(PAGE_TITLE)
    chart_name = st.sidebar.radio("Choose a page:", CHARTS, 0)
    if chart_name == "Welcome":
        st.write(
            "Welcome to the statistics site of TCCC Fantasy Football."
            "\n\nChoose a page in the sidebar!"
        )
    else:
        st.header(chart_name)
        chart_dict = CHARTS[chart_name]
        class_instance = chart_dict["class"](chart_dict["sql"])
        df = class_instance.get_df()
        class_instance.get_chart(
            df, *chart_dict["args"], **chart_dict["kwargs"]
        )


if __name__ == "__main__":
    run()
