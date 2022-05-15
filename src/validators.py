#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Validate data."""

# pylint: disable=invalid-name,unused-variable


import pandas as pd

from src.utils import log_prefect


def validate_merged_data(df: pd.DataFrame, use_prefect: bool = False) -> None:
    """Perform validation checks on merged ata."""
    log_prefect("Validating merged data...", True, use_prefect)
    start_station_ids = df["START_STATION_ID"]
    station_ids = df["STATION_ID"]
    try:
        assert (start_station_ids - station_ids).max() == 0
        if use_prefect:
            print(
                "Verified no difference between number of START_STATION_IDs "
                "and STATION_IDs"
            )
    except AssertionError as e:
        print(
            "Got non-zero difference between number of START_STATION_IDs and "
            "STATION_IDs"
        )
    log_prefect("Done with validation.", False, use_prefect)

    num_start_station_ids = df["START_STATION_ID"].nunique()
    num_station_ids = df["STATION_ID"].nunique()
    try:
        assert num_start_station_ids == num_station_ids
        print("Verified number of unique START_STATION_IDs and STATION_IDs")
    except AssertionError as e:
        print(
            f"Got mismatch between number of unique "
            f"START_STATION_IDs: {num_start_station_ids:,}, "
            f"STATION_IDs: {num_station_ids:,}"
        )


def validate_data_download_status(
    df_agg_dwnld: pd.DataFrame, df_statuses: pd.DataFrame
) -> None:
    """Validate logic for downloading ridership data."""
    if not df_agg_dwnld.empty:
        downloaded_files = (
            df_agg_dwnld["downloaded_file"].unique().to_numpy().tolist()
        )
        csv_filenames = df_agg_dwnld["csv_file"].unique().to_numpy().tolist()
        assert df_agg_dwnld["csv_file"].nunique() == 2
        assert downloaded_files == [True]
        assert df_agg_dwnld["last_modified_timestamp"].nunique() == 1

        assert [
            utype.to_numpy().tolist()
            for utype in df_agg_dwnld.groupby(["csv_file"])["USER_TYPE"]
            .unique()
            .tolist()
        ] == [["Annual Member", "Casual Member"]] * len(csv_filenames)
        num_utypes = (
            df_agg_dwnld.groupby(["csv_file"])["USER_TYPE"].nunique().tolist()
        )
        assert num_utypes == [2] * len(csv_filenames)
        for c in ["downloaded_file", "parquet_file_outdated"]:
            assert df_statuses[c].unique().to_numpy().tolist() == [True]
    else:
        assert df_statuses[
            "parquet_file_exists"
        ].unique().to_numpy().tolist() == [True]
        for c in ["downloaded_file", "parquet_file_outdated"]:
            assert df_statuses[c].unique().to_numpy().tolist() == [False]
