#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Plotting Charts with Streamlit."""

import os
from typing import Dict, Union

import dask.dataframe as dd
import pandas as pd
import snowflake.connector
from dask_snowflake import read_snowflake
from dotenv import find_dotenv, load_dotenv

# pylint: disable=invalid-name,broad-except


def get_snowflake_credentials() -> Dict[str, str]:
    """Acess Snowflake credentials from environment variables."""
    load_dotenv(find_dotenv())
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("DB_NAME"),
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    return snowflake_dict


def show_sql_df(query: str, connection) -> Union[None, pd.DataFrame]:
    """Fetch and display results of SQL query as pandas DataFrame."""
    try:
        df_query = pd.read_sql(query, con=connection)
        print(f"Got {len(df_query):,} rows of results")
        if not df_query.empty:
            return df_query
    except Exception as e:
        print(str(e))
    return None


def run_snowflake_query(query: str) -> pd.DataFrame:
    """Run a query against Snowflake database."""
    snowflake_dict = get_snowflake_credentials()
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    df = show_sql_df(query, cur)
    cur.close()
    conn.close()
    return df


def dask_show_query_df(
    query: str,
    snowflake_connection_dict: Dict,
    compute_dask: bool = False,
) -> dd:
    """Use Dask to query Snowflake and return query resultset as DDF."""
    ddf = read_snowflake(query, connection_kwargs=snowflake_connection_dict)
    if compute_dask:
        ddf = ddf.compute()
    return ddf
