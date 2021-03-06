#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare stations metadata."""

# pylint: disable=invalid-name


from typing import Dict, List

import geopandas as gpd
import pandas as pd
import pandera as pa
import requests

from src.utils import log_prefect

ch_essentials_schema = pa.DataFrameSchema(
    columns={
        "ID": pa.Column(pa.Int),
        "NAME": pa.Column(pd.StringDtype()),
        "POI_LATITUDE": pa.Column(
            pa.Float64,
            nullable=True,
        ),
        "POI_LONGITUDE": pa.Column(
            pa.Float64,
            nullable=True,
        ),
    },
    index=pa.Index(pa.Int),
)
poi_schema = pa.DataFrameSchema(
    columns={
        "ID": pa.Column(pa.Int, unique=True),
        "ADDRESS_INFO": pa.Column(pd.StringDtype()),
        "NAME": pa.Column(pd.StringDtype(), unique=True),
        "CATEGORY": pa.Column(pd.StringDtype()),
        "PHONE": pa.Column(pd.StringDtype()),
        "EMAIL": pa.Column(pd.StringDtype()),
        "WEBSITE": pa.Column(pd.StringDtype()),
        "GEOID": pa.Column(pa.Float, nullable=True),
        "RECEIVED_DATE": pa.Column(pd.StringDtype()),
        "ADDRESS_POINT_ID": pa.Column(pa.Float, nullable=True),
        "LINEAR_NAME_FULL": pa.Column(pd.StringDtype()),
        "ADDRESS_FULL": pa.Column(pd.StringDtype()),
        "POSTAL_CODE": pa.Column(pd.StringDtype()),
        "MUNICIPALITY": pa.Column(pd.StringDtype()),
        "CITY": pa.Column(pd.StringDtype()),
        "PLACE_NAME": pa.Column(pd.StringDtype()),
        "GENERAL_USE_CODE": pa.Column(pa.Float, nullable=True),
        "CENTRELINE": pa.Column(pa.Float, nullable=True),
        "LO_NUM": pa.Column(pa.Float, nullable=True),
        "LO_NUM_SUF": pa.Column(pd.StringDtype()),
        "HI_NUM": pa.Column(pd.StringDtype()),
        "HI_NUM_SUF": pa.Column(pd.StringDtype()),
        "LINEAR_NAME_ID": pa.Column(pa.Float, nullable=True),
        "WARD": pa.Column(pd.StringDtype()),
        "WARD_2003": pa.Column(pa.Float, nullable=True),
        "WARD_2018": pa.Column(pa.Float, nullable=True),
        "MI_PRINX": pa.Column(pa.Float, nullable=True),
        "ATTRACTION": pa.Column(pd.StringDtype(), unique=True),
        "MAP_ACCESS": pa.Column(pd.StringDtype()),
        "POI_LONGITUDE": pa.Column(pa.Float, unique=False),
        "POI_LATITUDE": pa.Column(pa.Float, unique=False),
    },
    index=pa.Index(pa.Int),
)
gdf_schema = pa.DataFrameSchema(
    columns={
        "AREA_ID": pa.Column(pa.Int),
        "AREA_SHORT_CODE": pa.Column(pd.StringDtype()),
        "AREA_LONG_CODE": pa.Column(pd.StringDtype()),
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "Shape__Area": pa.Column(pa.Float64),
        "AREA_LATITUDE": pa.Column(pa.Float64),
        "AREA_LONGITUDE": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
pub_trans_locations_schema = pa.DataFrameSchema(
    columns={
        "stop_id": pa.Column(pa.Int),
        "stop_code": pa.Column(pa.Int),
        "stop_name": pa.Column(pd.StringDtype()),
        "stop_desc": pa.Column(pd.StringDtype(), nullable=True),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
        "zone_id": pa.Column(pa.Float64, nullable=True),
        "stop_url": pa.Column(pd.StringDtype(), nullable=True),
        "location_type": pa.Column(pa.Float64, nullable=True),
        "parent_station": pa.Column(pa.Float64, nullable=True),
        "stop_timezone": pa.Column(pa.Float64, nullable=True),
        "wheelchair_boarding": pa.Column(pa.Int),
    },
    index=pa.Index(pa.Int),
)
coll_univ_schema = pa.DataFrameSchema(
    columns={
        "institution_id": pa.Column(pa.Int),
        "institution_name": pa.Column(pd.StringDtype()),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)


def get_lat_long(row):
    """Get latitude and longitude."""
    return row["coordinates"]


@pa.check_output(gdf_schema)
def get_neighbourhood_boundary_land_area_data(
    url: str, params: Dict, cols_to_keep: List[str], use_prefect: bool = False
) -> pd.DataFrame:
    """Get citywide neighbourhood boundaries."""
    log_prefect("Getting neighbourhood boundaries...", True, use_prefect)
    package = requests.get(url, params=params).json()
    files = package["result"]["resources"]
    n_url = [f["url"] for f in files if f["url"].endswith("4326.geojson")][0]
    gdf = gpd.read_file(n_url)
    # print(gdf.head(2))
    gdf["centroid"] = (
        gdf["geometry"].to_crs(epsg=3395).centroid.to_crs(epsg=4326)
    )
    gdf["AREA_LATITUDE"] = gdf["centroid"].y
    gdf["AREA_LONGITUDE"] = gdf["centroid"].x
    gdf["Shape__Area"] = gdf["geometry"].to_crs(epsg=3857).area
    gdf = gdf.astype(
        {
            "AREA_SHORT_CODE": pd.StringDtype(),
            "AREA_LONG_CODE": pd.StringDtype(),
            "AREA_NAME": pd.StringDtype(),
            "AREA_LATITUDE": float,
            "AREA_LONGITUDE": float,
        }
    )[cols_to_keep]
    log_prefect("Done.", False, use_prefect)
    return gdf


@pa.check_output(coll_univ_schema)
def get_coll_univ_locations(use_prefect: bool = False) -> pd.DataFrame:
    """Get college and university locations within city boundaries."""
    log_prefect("Getting college and univ locations...", True, use_prefect)
    coll_univ_locations = {
        "centennial": {"lat": 43.7854, "lon": -79.22664},
        "george-brown": {"lat": 43.6761, "lon": -79.4111},
        "humber": {"lat": 43.7290, "lon": -79.6074},
        "ocad": {"lat": 43.6530, "lon": -79.3912},
        "ryerson": {"lat": 43.6577, "lon": -79.3788},
        "seneca": {"lat": 43.7955, "lon": -79.3496},
        "tynedale": {"lat": 43.7970, "lon": -79.3945},
        "uoft-scarborough": {"lat": 43.7844, "lon": -79.1851},
        "uoft": {"lat": 43.6629, "lon": -79.5019},
        "yorku": {"lat": 43.7735, "lon": -79.5019},
        "yorku-glendon": {"lat": 43.7279, "lon": -79.3780},
    }
    df_coll_univ = (
        pd.DataFrame.from_dict(coll_univ_locations, orient="index")
        .reset_index()
        .rename(columns={"index": "institution_name"})
        .reset_index()
        .rename(columns={"index": "institution_id"})
    ).astype({"institution_name": pd.StringDtype()})
    log_prefect("Done.", False, use_prefect)
    return df_coll_univ
