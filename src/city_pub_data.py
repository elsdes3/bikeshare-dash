#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve bikeshare stations metadata."""

# pylint: disable=invalid-name


from io import BytesIO
from typing import Dict, List
from urllib.request import urlopen
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import pandera as pa
import requests

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
        "Shape__Length": pa.Column(pa.Float64),
        "LATITUDE": pa.Column(pd.StringDtype(), nullable=True),
        "AREA_LATITUDE": pa.Column(pa.Float64),
        "LONGITUDE": pa.Column(pd.StringDtype(), nullable=True),
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


@pa.check_output(poi_schema)
def get_poi_data(url: str, poi_params: Dict) -> pd.DataFrame:
    """Get points of interest within city boundaries."""
    poi_dtypes_dict = dict(
        ADDRESS_INFO=pd.StringDtype(),
        NAME=pd.StringDtype(),
        CATEGORY=pd.StringDtype(),
        PHONE=pd.StringDtype(),
        EMAIL=pd.StringDtype(),
        WEBSITE=pd.StringDtype(),
        RECEIVED_DATE=pd.StringDtype(),
        LINEAR_NAME_FULL=pd.StringDtype(),
        ADDRESS_FULL=pd.StringDtype(),
        POSTAL_CODE=pd.StringDtype(),
        MUNICIPALITY=pd.StringDtype(),
        CITY=pd.StringDtype(),
        PLACE_NAME=pd.StringDtype(),
        LO_NUM_SUF=pd.StringDtype(),
        HI_NUM=pd.StringDtype(),
        HI_NUM_SUF=pd.StringDtype(),
        WARD=pd.StringDtype(),
        ATTRACTION=pd.StringDtype(),
        MAP_ACCESS=pd.StringDtype(),
    )
    package = requests.get(url, params=poi_params).json()
    poi_url = package["result"]["resources"][0]["url"]
    df = pd.read_csv(poi_url)
    df = df.rename(columns={list(df)[0]: "ID"})

    df[["POI_LONGITUDE", "POI_LATITUDE"]] = pd.DataFrame(
        df["geometry"].apply(eval).apply(get_lat_long).tolist()
    )
    # Verify no duplicates (by name) are in the data
    assert df[df.duplicated(subset=["NAME"], keep=False)].empty
    df = df.astype(poi_dtypes_dict)
    return df


@pa.check_output(ch_essentials_schema)
def get_cultural_hotspots(url: str, params: Dict) -> pd.DataFrame:
    """Get cultural hotspots within city boundaries."""
    package = requests.get(url, params=params).json()
    ch_locations = package["result"]["resources"][0]["url"]
    ch_locs_dir_path = "data/raw/cultural-hotspot-points-of-interest-wgs84"
    with urlopen(ch_locations) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(ch_locs_dir_path)
    df = gpd.read_file(f"{ch_locs_dir_path}/CULTURAL_HOTSPOT_WGS84.shp")
    df = (
        df.drop_duplicates(
            subset=["PNT_OF_INT", "LATITUDE", "LONGITUDE"],
            keep="first",
        )
        .reset_index(drop=True)
        .copy()
    )
    df = (
        df.drop_duplicates(
            subset=["PNT_OF_INT"],
            keep="first",
        )
        .reset_index(drop=True)
        .copy()
    )
    assert df[df.duplicated(subset=["PNT_OF_INT"], keep=False)].empty
    df_essentials = (
        df[["RID", "PNT_OF_INT", "LATITUDE", "LONGITUDE"]]
        .rename(
            columns={
                "RID": "ID",
                "PNT_OF_INT": "NAME",
                "LATITUDE": "POI_LATITUDE",
                "LONGITUDE": "POI_LONGITUDE",
            }
        )
        .astype({"NAME": pd.StringDtype()})
    )
    # print(df_essentials.dtypes)
    return df_essentials


@pa.check_output(gdf_schema)
def get_neighbourhood_boundary_land_area_data(
    url: str, params: Dict, cols_to_keep: List[str]
) -> pd.DataFrame:
    """Get citywide neighbourhood boundaries."""
    package = requests.get(url, params=params).json()
    files = package["result"]["resources"]
    n_url = [f["url"] for f in files if f["url"].endswith("4326.geojson")][0]
    gdf = gpd.read_file(n_url)
    gdf["centroid"] = (
        gdf["geometry"].to_crs(epsg=3395).centroid.to_crs(epsg=4326)
    )
    gdf["AREA_LATITUDE"] = gdf["centroid"].y
    gdf["AREA_LONGITUDE"] = gdf["centroid"].x
    gdf = gdf.astype(
        {
            "AREA_SHORT_CODE": pd.StringDtype(),
            "AREA_LONG_CODE": pd.StringDtype(),
            "AREA_NAME": pd.StringDtype(),
            "LATITUDE": pd.StringDtype(),
            "LONGITUDE": pd.StringDtype(),
        }
    )[cols_to_keep]
    assert len(gdf) == 140
    return gdf


@pa.check_output(pub_trans_locations_schema)
def get_public_transit_locations(url: str, params: Dict) -> pd.DataFrame:
    """Get public transit locations within city boundaries."""
    package = requests.get(url, params=params).json()
    pt_locations = package["result"]["resources"][0]["url"]
    pt_locs_dir_path = "data/raw/opendata_ttc_schedules"
    with urlopen(pt_locations) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(pt_locs_dir_path)
    df_pt = pd.read_csv(f"{pt_locs_dir_path}/stops.txt").astype(
        {
            "stop_name": pd.StringDtype(),
            "stop_desc": pd.StringDtype(),
            "stop_url": pd.StringDtype(),
        }
    )
    df_pt = df_pt.rename(columns={"stop_lat": "lat", "stop_lon": "lon"})
    return df_pt


@pa.check_output(coll_univ_schema)
def get_coll_univ_locations() -> pd.DataFrame:
    """Get college and university locations within city boundaries."""
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
    return df_coll_univ
