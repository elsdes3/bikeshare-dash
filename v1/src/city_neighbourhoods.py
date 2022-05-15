#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve and Process city neighbourhood data."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=logging-fstring-interpolation


from typing import Dict

import geopandas as gpd
import pandas as pd
import pandera as pa
import prefect
import requests

neigh_demog_schema = pa.DataFrameSchema(
    columns={
        "name": pa.Column(pd.StringDtype()),
        "Neighbourhood Number": pa.Column(pd.StringDtype()),
        "Population, 2016": pa.Column(pd.StringDtype()),
        "Youth (15-24 years)": pa.Column(pd.StringDtype()),
        "Working Age (25-54 years)": pa.Column(pd.StringDtype()),
        "AREA_NAME": pa.Column(pd.StringDtype()),
    },
    index=pa.Index(pa.Int),
)


def get_toronto_open_data(
    url: str, params: Dict, col_rename_dict: Dict = {}
) -> pd.DataFrame:
    """Download a dataset from Toronto Open Data portal."""
    package = requests.get(url, params=params).json()
    datastore_url = (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/"
        "action/datastore_search"
    )
    for _, resource in enumerate(package["result"]["resources"]):
        if resource["datastore_active"]:
            url = datastore_url
            p = {"id": resource["id"]}
            data = requests.get(url, params=p).json()
            df = pd.DataFrame(data["result"]["records"])
            break
    if col_rename_dict:
        df = df.rename(columns=col_rename_dict)
    return df


@pa.check_output(neigh_demog_schema)
def get_neighbourhood_profile_data(url: str, params: Dict) -> pd.DataFrame:
    """Get neighbourhood profile dataset from Toronto Open Data portal."""
    df_neigh_demog = get_toronto_open_data(url, params)
    df_neigh_demog = (
        df_neigh_demog[
            df_neigh_demog["Characteristic"].isin(
                [
                    "Neighbourhood Number",
                    "Youth (15-24 years)",
                    "Working Age (25-54 years)",
                    "Population, 2016",
                ]
            )
        ]
        .iloc[:, slice(4, None)]
        .set_index("Characteristic")
        .T.reset_index()
        .iloc[1:]
        .reset_index(drop=True)
        .rename(columns={"index": "name"})
    )
    assert len(df_neigh_demog) == 140
    df_neigh_demog["AREA_NAME"] = (
        df_neigh_demog["name"]
        + " ("
        + df_neigh_demog["Neighbourhood Number"]
        + ")"
    )
    df_neigh_demog = df_neigh_demog.astype(
        {
            "name": pd.StringDtype(),
            "Neighbourhood Number": pd.StringDtype(),
            "Population, 2016": pd.StringDtype(),
            "Youth (15-24 years)": pd.StringDtype(),
            "Working Age (25-54 years)": pd.StringDtype(),
            "AREA_NAME": pd.StringDtype(),
        }
    )
    return df_neigh_demog


def get_neighbourhood_containing_point(
    gdf: gpd.GeoDataFrame,
    df: pd.DataFrame,
    lat: str = "Latitude",
    lon: str = "Longitude",
    crs: int = 4326,
) -> gpd.GeoDataFrame:
    """Get name of Toronto neighbourhood containing a point co-ordinate."""
    cols_order = list(df) + list(gdf)
    polygons_contains = (
        gpd.sjoin(
            gdf,
            gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=crs
            ),
            predicate="contains",
        )
        .reset_index(drop=True)
        .drop(columns=["index_right"])[cols_order]
    )
    return polygons_contains


def get_data_with_neighbourhood(
    gdf: gpd.GeoDataFrame,
    df: pd.DataFrame,
    lat: int,
    lon: int,
    col_to_join: str,
    crs: int = 4326,
    use_prefect: bool = False,
) -> gpd.GeoDataFrame:
    """Add city neighourhood name to bikeshare data."""
    cols_to_keep = [col_to_join, "AREA_NAME", "geometry", "Shape__Area"]
    df_check = get_neighbourhood_containing_point(gdf, df, lat, lon, crs)[
        cols_to_keep
    ]
    df = df.merge(
        df_check.drop(columns=["geometry"]), on=col_to_join, how="left"
    ).drop(columns=["geometry"])
    df = df.dropna(subset=["AREA_NAME"])
    num_dropped_rows = len(df[["AREA_NAME"]].isna().sum())
    loop_str = f"Dropped {num_dropped_rows} rows with a missing AREA_NAME"
    if use_prefect:
        logger = prefect.utilities.logging.get_logger()
        logger.info(loop_str)
    else:
        print(loop_str)
    return df
