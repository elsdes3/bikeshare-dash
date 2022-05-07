#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Streamlit dashboard configuration."""

# pylint: disable=invalid-name,broad-except,redefined-outer-name
# pylint: disable=consider-using-f-string

import argparse

import altair as alt
import pandas as pd
import streamlit as st

BACKGROUND_COLOR = "white"
COLOR = "black"


@st.cache(ttl=24 * 60 * 60)
def filter_by_col(col: str, value: int) -> pd.DataFrame:
    """Filter data by a column."""
    dff = df[df[col].dt.hour <= value]
    return dff


def configure_page() -> None:
    """Configure page setup using HTML."""
    # Set wide page format
    st.set_page_config(layout="wide")
    # Remove whitespace from the top of the page and sidebar
    st.markdown(
        """
        <style>
            .css-18e3th9 {
                    padding-top: 0rem;
                    padding-bottom: 2rem;
                    padding-left: 3rem;
                    padding-right: 3rem;
                }
            .css-1d391kg {
                    padding-top: 3.5rem;
                    padding-right: 1rem;
                    padding-bottom: 3.5rem;
                    padding-left: 1rem;
                }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache
def load_data(url: str, nrows: int, date_col: str) -> pd.DataFrame:
    """Get data."""
    df = pd.read_csv(url, nrows=nrows)
    df.columns = df.columns.str.lower()
    df[date_col] = pd.to_datetime(df[date_col])
    # query = f"""
    #         SELECT *
    #         FROM {args.trips_table_name}
    #         """
    # df = phw.run_snowflake_query(query)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trips-table-name",
        type=str,
        dest="trips_table_name",
        default="trips",
        help="name of trips table",
    )
    args = parser.parse_args()

    DATE_COLUMN = "date/time"
    DATA_URL = (
        "https://s3-us-west-2.amazonaws.com/"
        "streamlit-demo-data/uber-raw-data-sep14.csv.gz"
    )
    N_ROWS = 80_000
    N_ROWS_TO_SHOW = 10

    configure_page()
    st.title("Uber pickups in NYC")

    # data_load_state = st.text("Loading data...")
    df = load_data(DATA_URL, N_ROWS, DATE_COLUMN)
    # data_load_state.text("Done! (using st.cache)")

    if st.checkbox("Show raw data"):
        st.subheader(f"Showing {len(df.head(N_ROWS_TO_SHOW)):,} rows of data")
        st.write(df.head(N_ROWS_TO_SHOW))

    st_obj = st.sidebar
    st_obj.markdown("## PAGE TITLE")

    # Some number in the range 0-23
    hour_to_filter = st.sidebar.slider("HOUR OF DAY", 0, 23, 17)

    filtered_data = filter_by_col(DATE_COLUMN, hour_to_filter)

    # st.subheader("Number of pickups by hour")
    st.markdown("#### Number of pickups by hour")

    base = alt.Chart(
        filtered_data.assign(hour=filtered_data[DATE_COLUMN].dt.hour)
    )
    chart = base.mark_bar().encode(
        x=alt.X("hour:Q", title="Hour of Day", bin=True),
        y="count()",
        tooltip=["hour"],
    )
    st.altair_chart(chart, use_container_width=True)
