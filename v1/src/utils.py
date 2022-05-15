#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Utilities to summarize, validate and display DataFrames."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=logging-fstring-interpolation


from typing import List, Union

import pandas as pd
import pandera as pa
import prefect
from IPython.display import display


def summarize_df(df: pd.DataFrame) -> None:
    """Show properties of a DataFrame."""
    display(
        df.dtypes.rename("dtype")
        .to_frame()
        .merge(
            df.isna().sum().rename("num_missing").to_frame(),
            left_index=True,
            right_index=True,
            how="left",
        )
        .assign(num=len(df))
        .merge(
            df.nunique().rename("nunique").to_frame(),
            left_index=True,
            right_index=True,
            how="left",
        )
        .merge(
            df.dropna(how="any")
            .sample(1)
            .squeeze()
            .rename("single_non_nan_value")
            .to_frame(),
            left_index=True,
            right_index=True,
            how="left",
        )
    )


def show_sql_df(
    query: str,
    cursor,
    table_output: bool = False,
) -> Union[None, pd.DataFrame]:
    """Fetch and display results of SQL query."""
    cursor.execute(query)
    if table_output:
        colnames = [cdesc[0].lower() for cdesc in cursor.description]
        cur_fetched = cursor.fetchall()
        if cur_fetched:
            df_query_output = pd.DataFrame.from_records(
                cur_fetched, columns=colnames
            )
            print(f"Got {len(df_query_output):,} rows of results")
            display(df_query_output)
            return df_query_output
    return None


def pandera_validate_data(
    df: pd.DataFrame, schema: pa.DataFrameSchema, ds_name: str
) -> None:
    """Manually validate a DataFrame using a pandera DataFrameSchema."""
    schema_cols = list(schema.columns)
    try:
        assert schema(df[schema_cols]).equals(df[schema_cols])
        print(f"Validated {ds_name} data")
    except pa.errors.SchemaError as e:
        print(f"Could not validate {ds_name} data\n{str(e)}")


def export_df_to_multiple_csv_files(
    df: pd.DataFrame,
    cols_to_export: List,
    fname_prefix: str = "local_stage",
    nrows_per_file: int = 100_000,
    use_prefect: bool = False,
):
    """Export DataFrame to multiple CSV files ."""
    max_indexes = list(range(nrows_per_file, len(df), nrows_per_file))
    for k, idx_max in enumerate(max_indexes, 1):
        start = idx_max - nrows_per_file
        end = idx_max if k < len(max_indexes) else len(df)
        df.iloc[start:end][cols_to_export].to_csv(
            f"data/processed/{fname_prefix}_{k}.csv.gz",
            compression="gzip",
            index=False,
        )
        loop_str = (
            f"Exported manual chunk {k} of {len(max_indexes):,} to "
            f"{fname_prefix}_{k}.csv.gz (indexes range = {start:,} - {end:,})"
        )
        if use_prefect:
            logger = prefect.utilities.logging.get_logger()
            logger.info(loop_str)
        else:
            print(loop_str)
