from abc import ABC, abstractmethod
from pathlib import Path

import streamlit as st
from google.cloud import bigquery
import pandas as pd
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
            "Reg. Season Win Rate",
            "Made Playoffs Rate",
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
                )
            )
        fig.update_traces(
            showlegend=False,
            boxpoints=False,
            fillcolor="rgba(0, 0, 0, 0)",
        )
        fig.update_layout(
            font={"color": st.get_option("theme.textColor")},
            title_text="All Time Scoring (hover to see exact values)",
            title_x=0.5,
            xaxis_title_text="Weekly Points Scored",
            xaxis_title_font={"size": 16},
            xaxis_tickfont={"size": 16},
            xaxis_showgrid=True,
            xaxis_gridcolor="rgb(128, 128, 128)",
            xaxis_gridwidth=1,
            xaxis_zeroline=False,
            yaxis_title_text="Manager",
            yaxis_title_font={"size": 16},
            yaxis_tickfont={"size": 16},
            margin={"l": 30, "r": 30, "t": 30, "b": 30},
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
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


class TopSingleWeekScores(Charter):
    def get_chart(self, df, *args, **kwargs):
        mutable_df = df.copy()
        mutable_df.columns = [
            "Manager",
            "Points",
            "Year",
            "Week",
        ]
        mutable_df["concat"] = (
            mutable_df["Manager"] + mutable_df["Year"] + mutable_df["Week"]
        )
        mutable_df = mutable_df.set_index("concat")
        mutable_df = pd.DataFrame(mutable_df["Points"]).style.set_table_styles(
            utils.CENTER_ALIGN_TABLE_TEXT + utils.INCREASE_TABLE_FONT_SIZE
        )
        st.table(mutable_df)


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
