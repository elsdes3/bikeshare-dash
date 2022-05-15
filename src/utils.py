#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Utilities to summarize, validate and display DataFrames."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=logging-fstring-interpolation


import pandas as pd
import pandera as pa
from IPython.display import display
from prefect import get_run_logger


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


def log_prefect(
    msg: str, start: bool = True, use_prefect: bool = False
) -> None:
    """Logging with Prefect."""
    if use_prefect:
        if start:
            logger = get_run_logger()
        logger.info(msg)
    else:
        print(msg)


def pandera_validate_data(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    ds_name: str,
    use_prefect: bool = False,
) -> None:
    """Manually validate a DataFrame using a pandera DataFrameSchema."""
    log_prefect(f"Validating {ds_name}...", True, use_prefect)
    schema_cols = list(schema.columns)
    try:
        schema(df[schema_cols])
        if not use_prefect:
            print(f"Validated {ds_name} data")
    except pa.errors.SchemaError as e:
        print(f"Could not validate {ds_name} data\n{str(e)}")
    log_prefect(f"Done validating {ds_name}.", False, use_prefect)


def save_data_to_parquet_file(
    df: pd.DataFrame, filepath: str = "data/raw/myfile.parquet.gzip"
) -> None:
    """Export DataFrame to a parquet file."""
    df.to_parquet(filepath, index=False, engine="auto")
