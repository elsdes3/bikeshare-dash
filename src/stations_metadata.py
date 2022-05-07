#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare stations metadata."""

# pylint: disable=invalid-name


from typing import Dict, List

import pandas as pd
import pandera as pa
import requests

stations_schema = pa.DataFrameSchema(
    columns={
        "station_id": pa.Column(pa.Int),
        "name": pa.Column(pd.StringDtype()),
        "physical_configuration": pa.Column(pd.StringDtype()),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
        "altitude": pa.Column(pa.Float64, nullable=True),
        "address": pa.Column(pd.StringDtype()),
        "capacity": pa.Column(pa.Int),
        "physicalkey": pa.Column(pa.Int),
        "transitcard": pa.Column(pa.Int),
        "creditcard": pa.Column(pa.Int),
        "phone": pa.Column(pa.Int),
    },
    index=pa.Index(pa.Int),
)


def get_stations_metadata(
    stations_url: str, stations_params: Dict
) -> pd.DataFrame:
    """Get bikeshare stations metadata from JSON feed."""
    package = requests.get(stations_url, params=stations_params).json()
    resources = package["result"]["resources"]
    df_about = pd.DataFrame.from_records(resources)
    r = requests.get(df_about["url"].tolist()[0]).json()
    url_stations = r["data"]["en"]["feeds"][2]["url"]
    df_stations = pd.DataFrame.from_records(
        requests.get(url_stations).json()["data"]["stations"]
    )
    df_stations = df_stations.astype(
        {
            "physical_configuration": pd.StringDtype(),
            "name": pd.StringDtype(),
            "address": pd.StringDtype(),
        }
    )
    return df_stations


@pa.check_output(stations_schema)
def transform_metadata(
    df: pd.DataFrame, stations_cols_wanted: List[str]
) -> pd.DataFrame:
    """Transform station metadata."""
    df["station_id"] = df["station_id"].astype(int)
    dfa = pd.DataFrame(
        df.set_index("station_id")["rental_methods"].tolist(),
        columns=["key", "transitcard", "creditcard", "phone"],
    )
    for c in ["KEY", "TRANSITCARD", "CREDITCARD", "PHONE"]:
        dfa[c.lower()] = dfa[c.lower()].map({c: 1}).fillna(0).astype(int)
    df = pd.concat(
        [
            df.drop(columns=["groups", "rental_methods"]),
            dfa,
        ],
        axis=1,
    ).rename(columns={"key": "physicalkey"})[stations_cols_wanted]
    return df
