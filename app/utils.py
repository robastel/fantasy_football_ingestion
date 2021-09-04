import streamlit as st

CENTER_ALIGN_TABLE_TEXT = [
    {
        "selector": "td",
        "props": [
            ("text-align", "center"),
        ],
    },
    {
        "selector": "th",
        "props": [
            ("text-align", "center"),
        ],
    },
]

INCREASE_TABLE_FONT_SIZE = [
    {
        "selector": "td",
        "props": [
            ("font-size", "1.25em"),
        ],
    },
    {
        "selector": "th",
        "props": [
            ("font-size", "1.25em"),
        ],
    },
]

PLOTLY_DEFAULT_LAYOUT_KWARGS = {
    "title_x": 0.5,
    "margin": {"l": 30, "r": 30, "t": 30, "b": 30},
    "plot_bgcolor": "rgba(0, 0, 0, 0)",
    "paper_bgcolor": "rgba(0, 0, 0, 0)",
    "font_color": st.get_option("theme.textColor"),
    # x axis
    "xaxis_title_font": {"size": 16},
    "xaxis_tickfont": {"size": 16},
    "xaxis_gridcolor": "rgb(128, 128, 128)",
    # y axis
    "yaxis_title_font": {"size": 16},
    "yaxis_tickfont": {"size": 16},
    "yaxis_gridcolor": "rgb(128, 128, 128)",
    # legend
    "legend_font_size": 16,
}
