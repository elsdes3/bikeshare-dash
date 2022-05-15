#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare trips data."""

# pylint: disable=invalid-name


import os
import re
from glob import glob
from typing import Dict, List
from zipfile import ZipFile

import pandas as pd
import pandera as pa
import requests

from src.utils import log_prefect

trips_schema = pa.DataFrameSchema(
    columns={
        "TRIP_ID": pa.Column(pa.Int),
        "TRIP__DURATION": pa.Column(pa.Int),
        "START_STATION_ID": pa.Column(
            pa.Int,
            nullable=True,
        ),
        "START_TIME": pa.Column(
            pa.Timestamp,
            checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))],
        ),
        "START_STATION_NAME": pa.Column(pd.StringDtype()),
        "END_STATION_ID": pa.Column(
            pa.Int,
            nullable=True,
        ),
        "END_TIME": pa.Column(
            pa.Timestamp,
            checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))],
        ),
        "END_STATION_NAME": pa.Column(pd.StringDtype()),
        "BIKE_ID": pa.Column(pa.Int, nullable=True),
        "USER_TYPE": pa.Column(
            pd.StringDtype(),
            checks=[
                pa.Check(
                    lambda s: s.isin(["Annual Member", "Casual Member"]),
                )
            ],
        ),
    },
    index=pa.Index(pa.Int),
)
urls_schema = pa.DataFrameSchema(
    columns={
        "url": pa.Column(pd.StringDtype()),
        "name": pa.Column(pd.StringDtype()),
        "format": pa.Column(pd.StringDtype()),
        "state": pa.Column(pd.StringDtype()),
    },
    index=pa.Index(pa.Int),
)
get_data_status_schema = pa.DataFrameSchema(
    columns={
        "trips_file_name": pa.Column(pd.StringDtype()),
        "last_modified_opendata": pa.Column(
            pd.DatetimeTZDtype(tz="America/Toronto")
        ),
        "parquet_file_exists": pa.Column(pd.BooleanDtype()),
        "parquet_file_outdated": pa.Column(pd.BooleanDtype()),
        "downloaded_file": pa.Column(pd.BooleanDtype()),
    },
    index=pa.Index(pa.Int),
)
raw_trips_schema = pa.DataFrameSchema(
    columns={
        "TRIP_ID": pa.Column(pa.Int),
        "TRIP__DURATION": pa.Column(pa.Int),
        "START_STATION_ID": pa.Column(pa.Int, nullable=True),
        "START_STATION_NAME": pa.Column(pd.StringDtype()),
        "START_TIME": pa.Column(
            pa.Timestamp,
            checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))],
        ),
        "USER_TYPE": pa.Column(
            pd.StringDtype(),
            checks=[
                pa.Check(
                    lambda s: s.isin(["Annual Member", "Casual Member"]),
                )
            ],
        ),
    },
    index=pa.Index(pa.Int),
)


def get_local_csv_list(
    raw_data_dir: str, years_wanted: List[int], use_prefect: bool = False
) -> List[str]:
    """Getting list of local CSV data files."""
    log_prefect("Getting list of local CSV data files...", True, use_prefect)
    files_by_year = [glob(f"{raw_data_dir}/*{y}*.csv") for y in years_wanted]
    csvs = sorted([f for files_list in files_by_year for f in files_list])
    log_prefect("Done.", False, use_prefect)
    return csvs


def get_ridership_data(
    raw_data_dir: str,
    url: str,
    last_modified_timestamp: pd.Timestamp,
    use_prefect: bool = False,
) -> Dict[str, str]:
    """Download bikeshare trips data."""
    # Split URL to get the file name
    file_name = os.path.basename(url)
    year = os.path.splitext(file_name)[0].split("-")[-1]
    zip_filepath = os.path.join(raw_data_dir, file_name)
    destination_dir = os.path.abspath(os.path.join(zip_filepath, os.pardir))

    # Check if previously downloaded contents are up-to-dte
    parquet_data_filepath = os.path.join(raw_data_dir, "agg_data.parquet.gzip")
    has_parquet = os.path.exists(parquet_data_filepath)
    if has_parquet:
        parquet_file_modified_time = (
            pd.read_parquet(parquet_data_filepath)
            .iloc[0]
            .loc["last_modified_timestamp"]
        )
        # print(last_modified_timestamp, parquet_file_modified_time)
        parquet_file_outdated_check = (
            os.path.exists(zip_filepath)
            and parquet_file_modified_time < last_modified_timestamp
        )
    else:
        parquet_file_modified_time = pd.NaT
        parquet_file_outdated_check = True

    if not has_parquet or parquet_file_outdated_check:
        log_prefect(
            f"Downloading raw data file {file_name}...", True, use_prefect
        )
        # print(zip_filepath, destination_dir, dest_filepath, existing_file)

        if not os.path.exists(zip_filepath):
            r = requests.get(url)

            # Writing the file to the local file system
            with open(zip_filepath, "wb") as output_file:
                output_file.write(r.content)

        csv_files = glob(f"{raw_data_dir}/*{year}-*.csv")
        # print(csv_files)
        if not csv_files:
            with ZipFile(zip_filepath, "r") as zipObj:
                # Extract all the contents of zip file
                zipObj.extractall(destination_dir)

        status_dict = dict(
            trips_file_name=file_name,
            last_modified_opendata=last_modified_timestamp,
            parquet_file_exists=has_parquet,
            parquet_file_data_last_modified=parquet_file_modified_time,
            parquet_file_outdated=parquet_file_outdated_check,
            downloaded_file=True,
        )
        log_prefect("Done downloading raw data.", False, use_prefect)
    else:
        log_prefect(
            f"Found the most recent version of {file_name} locally. "
            "Did nothing.",
            True,
            use_prefect,
        )
        status_dict = dict(
            trips_file_name=file_name,
            last_modified_opendata=last_modified_timestamp,
            parquet_file_exists=has_parquet,
            parquet_file_data_last_modified=parquet_file_modified_time,
            parquet_file_outdated=parquet_file_outdated_check,
            downloaded_file=False,
        )
    return status_dict


@pa.check_output(urls_schema)
def get_file_urls(
    main_dataset_url: str,
    dataset_params: Dict,
    years_wanted: Dict[int, List],
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Get list of ridership file URLs."""
    log_prefect("Retrieving data URLs...", True, use_prefect)
    package = requests.get(main_dataset_url, params=dataset_params).json()
    resources = package["result"]["resources"]
    df = (
        pd.DataFrame.from_records(resources)[
            ["last_modified", "url", "name", "format", "state"]
        ]
        .rename(columns={"last_modified": "last_modified_opendata"})
        .astype(
            {
                "url": pd.StringDtype(),
                "name": pd.StringDtype(),
                "format": pd.StringDtype(),
                "state": pd.StringDtype(),
            }
        )
    )
    df["last_modified_opendata"] = pd.to_datetime(df["last_modified_opendata"])
    years_wanted_str = "|".join([str(year) for year in years_wanted])
    df_filtered = df.query("name.str.contains(@years_wanted_str)")
    log_prefect("Done.", False, use_prefect)
    return df_filtered


@pa.check_output(get_data_status_schema)
def get_data_zip_file_download_status(
    data_all_urls: pd.DataFrame, raw_data_dir: str, use_prefect: bool = False
) -> pd.DataFrame:
    """Check whether ."""
    log_prefect(
        "Getting trips data and and modified status...", True, use_prefect
    )
    status_dicts = []
    for _, row in data_all_urls.iterrows():
        status_dict = get_ridership_data(
            raw_data_dir,
            row["url"],
            row["last_modified_opendata"].tz_localize("America/Toronto"),
            use_prefect,
        )
        status_dicts.append(status_dict)
    data_status = pd.DataFrame.from_records(status_dicts).astype(
        {
            "trips_file_name": pd.StringDtype(),
            "downloaded_file": pd.BooleanDtype(),
            "parquet_file_exists": pd.BooleanDtype(),
            "parquet_file_outdated": pd.BooleanDtype(),
        }
    )
    data_status["last_modified_opendata"] = pd.to_datetime(
        data_status["last_modified_opendata"]
    )
    log_prefect("Done.", False, use_prefect)
    return data_status


def read_data(
    fpath: str,
    dtypes_dict: Dict,
    date_cols: List[str],
    nan_cols: List[str],
    duplicated_cols: List[str],
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Read single month's ridership data, drop NaNs and export to CSV."""
    log_prefect(f"Reading ridership data from {fpath}...", True, use_prefect)
    df = pd.read_csv(
        fpath,
        encoding="cp1252",
        parse_dates=date_cols,
        dtype=dtypes_dict,
    )
    df.columns = [re.sub(r"[^A-Za-z0-9\s]+", "", c) for c in list(df)]
    df.columns = [
        re.sub(r"[^A-Za-z0-9\s]+", "", c).replace(" ", "_").upper()
        for c in list(df)
    ]
    df.columns = df.columns.str.replace(" ", "_").str.upper()
    df = (
        df[
            [
                "TRIP_ID",
                "TRIP__DURATION",
                "START_STATION_ID",
                "START_STATION_NAME",
                "START_TIME",
                "USER_TYPE",
            ]
        ]
        .dropna(subset=nan_cols)
        .drop_duplicates(subset=duplicated_cols, keep="first")
    )
    log_prefect("Done.", False, use_prefect)
    return df


@pa.check_output(raw_trips_schema)
def get_single_ridership_data_file(
    url: str,
    dtypes_dict: Dict,
    date_cols: List[str],
    nan_cols: List[str],
    duplicated_cols: List[str],
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Get single month's ridership data."""
    log_prefect("Retrieving monthly trips data...", True, use_prefect)
    fname = os.path.basename(url)
    if not use_prefect:
        print(f"Loading data from {fname}...", end="")
    df_trips_data = read_data(
        url, dtypes_dict, date_cols, nan_cols, duplicated_cols
    )
    if not use_prefect:
        print("Done.")
    log_prefect("Done.", False, use_prefect)
    return df_trips_data
