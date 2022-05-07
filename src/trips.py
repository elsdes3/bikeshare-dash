#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare trips data."""

# pylint: disable=invalid-name


import os
import re
from glob import glob
from multiprocessing import cpu_count
from typing import Dict, List, Union

import pandas as pd
import pandera as pa
import requests
from joblib import Parallel, delayed

# station_ids = df_stations["station_id"].unique().tolist()
trips_schema = pa.DataFrameSchema(
    columns={
        "TRIP_ID": pa.Column(pa.Int),
        "TRIP__DURATION": pa.Column(pa.Int),
        "START_STATION_ID": pa.Column(
            pa.Int,
            nullable=True,
            # checks=[
            #     pa.Check(
            #         lambda s: (s.min() >= min(station_ids))
            #         & (s.max() <= max(station_ids))
            #     )
            # ],
        ),
        "START_TIME": pa.Column(
            pa.Timestamp,
            checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))],
        ),
        "START_STATION_NAME": pa.Column(pd.StringDtype()),
        "END_STATION_ID": pa.Column(
            pa.Int,
            nullable=True,
            # checks=[
            #     pa.Check(
            #         lambda s: (s.min() >= min(station_ids))
            #         & (s.max() <= max(station_ids))
            #     )
            # ],
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


def get_file_urls(
    main_dataset_url: str, dataset_params: Dict, years_wanted: Dict[int, List]
) -> List:
    """Get list of ridership file URLs."""
    package = requests.get(main_dataset_url, params=dataset_params).json()
    resources = package["result"]["resources"]
    df = pd.DataFrame.from_records(resources)
    year_month_wanted = [
        f"{y}-{str(m).zfill(2)}" for y, ms in years_wanted.items() for m in ms
    ]
    year_month_wanted_str = "|".join(year_month_wanted)
    urls_list = df.query("name.str.contains(@year_month_wanted_str)")[
        "url"
    ].tolist()
    return urls_list


def read_data(
    url: str, dtypes_dict: Dict, date_cols: List[str], nan_cols: List[str]
) -> Dict[str, Union[List[str], int]]:
    """Read single month's ridership data, drop NaNs and export to CSV."""
    df = pd.read_csv(
        url,
        encoding="cp1252",
        parse_dates=date_cols,
        dtype=dtypes_dict,
    ).dropna(subset=nan_cols)
    # df.columns = [re.sub("[^A-Za-z0-9\s]+", "", c) for c in list(df)]
    df.columns = [
        re.sub(r"[^A-Za-z0-9\s]+", "", c).replace(" ", "_").upper()
        for c in list(df)
    ]
    df.columns = df.columns.str.replace(" ", "_").str.upper()
    fpath = f"data/raw/{os.path.basename(url).replace('.csv', '')}.csv"
    if not os.path.exists(fpath):
        df.to_csv(fpath, index=False)
    return {os.path.basename(url): {"columns": list(df), "nrows": len(df)}}


def get_single_ridership_data_file(
    url: str, dtypes_dict: Dict, date_cols: List[str], nan_cols: List[str]
) -> Dict[str, List[str]]:
    """Get single month's ridership data."""
    fname = os.path.basename(url)
    print(f"Loading data from {fname}...", end="")
    cols_dict = read_data(url, dtypes_dict, date_cols, nan_cols)
    print("Done.")
    return cols_dict


def get_all_data_files(
    urls_list: List,
    dtypes_dict: Dict,
    date_cols: List[str],
    nan_cols: List[str],
    parallel: bool = False,
) -> Dict[str, List[str]]:
    """Get all months' ridership data."""
    if parallel:
        executor = Parallel(n_jobs=cpu_count(), backend="multiprocessing")
        tasks = (
            delayed(get_single_ridership_data_file)(
                url, dtypes_dict, date_cols, nan_cols
            )
            for url in urls_list
        )
        cols_dicts = executor(tasks)
    else:
        cols_dicts = [
            get_single_ridership_data_file(
                url, dtypes_dict, date_cols, nan_cols
            )
            for url in urls_list
        ]
    return cols_dicts


@pa.check_output(trips_schema)
def load_trips_data(glob_str: str) -> pd.DataFrame:
    """Load all ridership CSVs into single DataFrame."""
    dtypes_dict_trips_transformed = {
        "TRIP_ID": pd.Int64Dtype(),
        "TRIP _DURATION": pd.Int64Dtype(),
        "START_STATION_ID": pd.Int64Dtype(),
        "START_STATION_NAME": pd.StringDtype(),
        "END_STATION_ID": pd.Int64Dtype(),
        "END_STATION_NAME": pd.StringDtype(),
        "BIKE_ID": pd.Int64Dtype(),
        "USER_TYPE": pd.StringDtype(),
    }
    dfs = [
        pd.read_csv(
            f,
            dtype=dtypes_dict_trips_transformed,
            parse_dates=["START_TIME", "END_TIME"],
        )
        for f in glob(glob_str)
    ]
    df = pd.concat(dfs, ignore_index=True)
    return df
