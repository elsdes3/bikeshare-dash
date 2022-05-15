#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Retrieve and Process city neighbourhood data."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=logging-fstring-interpolation


import geopandas as gpd
import pandas as pd
import prefect

from src.utils import log_prefect


def get_neighbourhood_containing_point(
    gdf: gpd.GeoDataFrame,
    df: pd.DataFrame,
    lat: str = "Latitude",
    lon: str = "Longitude",
    crs: int = 4326,
    use_prefect: bool = False,
) -> gpd.GeoDataFrame:
    """Get name of Toronto neighbourhood containing a point co-ordinate."""
    log_prefect("Extracting neighbourhood name...", True, use_prefect)
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
    log_prefect("Done.", False, use_prefect)
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
    log_prefect("Adding neighbourhood to trips data...", True, use_prefect)
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
    log_prefect("Done.", False, use_prefect)
    return df
