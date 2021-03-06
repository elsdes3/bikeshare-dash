#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare stations metadata."""

# pylint: disable=invalid-name


from typing import Dict, List

import pandas as pd
import pandera as pa
import requests

from src.utils import log_prefect

raw_stations_schema = pa.DataFrameSchema(
    columns={
        # "station_id": pa.Column(pa.Int),
        "name": pa.Column(pd.StringDtype()),
        "physical_configuration": pa.Column(pd.StringDtype()),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
        "altitude": pa.Column(pa.Float64, nullable=True),
        "address": pa.Column(pd.StringDtype()),
        "capacity": pa.Column(pa.Int),
        "is_charging_station": pa.Column(pd.BooleanDtype()),
        "rental_methods": pa.Column(pa.Object),
        "groups": pa.Column(pa.Object),
        "obcn": pa.Column(pd.StringDtype()),
        "nearby_distance": pa.Column(pa.Float),
        "post_code": pa.Column(pd.StringDtype(), nullable=True),
        "cross_street": pa.Column(pd.StringDtype(), nullable=True),
    },
    index=pa.Index(pa.Int),
)
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


@pa.check_output(raw_stations_schema)
def get_stations_metadata(
    stations_url: str, stations_params: Dict, use_prefect: bool = False
) -> pd.DataFrame:
    """Get bikeshare stations metadata from JSON feed."""
    log_prefect("Retrieving stations metadata...", True, use_prefect)
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
            # "station_id": pd.Int64Dtype(),
            "physical_configuration": pd.StringDtype(),
            "name": pd.StringDtype(),
            "is_charging_station": pd.BooleanDtype(),
            "address": pd.StringDtype(),
            "obcn": pd.StringDtype(),
            "post_code": pd.StringDtype(),
            "cross_street": pd.StringDtype(),
        }
    )
    log_prefect("Done.", False, use_prefect)
    return df_stations


@pa.check_io(data=raw_stations_schema, out=stations_schema)
def transform_metadata(
    data: pd.DataFrame,
    stations_cols_wanted: List[str],
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Transform station metadata."""
    log_prefect("Transforming stations metadata...", True, use_prefect)
    data["station_id"] = data["station_id"].astype(int)
    dfa = pd.DataFrame(
        data.set_index("station_id")["rental_methods"].tolist(),
        columns=["key", "transitcard", "creditcard", "phone"],
    )
    for c in ["KEY", "TRANSITCARD", "CREDITCARD", "PHONE"]:
        dfa[c.lower()] = dfa[c.lower()].map({c: 1}).fillna(0).astype(int)
    data = pd.concat(
        [
            data.drop(columns=["groups", "rental_methods"]),
            dfa,
        ],
        axis=1,
    ).rename(columns={"key": "physicalkey"})[stations_cols_wanted]
    assert data["station_id"].nunique() == len(data)
    log_prefect("Done.", False, use_prefect)
    return data
