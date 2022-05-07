#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Data pipeline utilities."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=too-many-arguments

from typing import Dict, List, Tuple

import pandas as pd
import pandera as pa
import prefect
from prefect import task

import src.aggregate_data as ad
import src.city_neighbourhoods as cn
import src.city_pub_data as cpd
import src.trips as bt
from src.process_trips import process_trips_data
from src.stations_metadata import get_stations_metadata, transform_metadata
from src.utils import export_df_to_multiple_csv_files


@task
def get_bikeshare_stations_metadata(
    open_tor_data_url: str,
    stations_params: Dict[str, str],
    stations_cols_wanted: List[str],
) -> pd.DataFrame:
    """Retrieve and process bikeshare stations metadata."""
    logger = prefect.context.get("logger")
    df = get_stations_metadata(open_tor_data_url, stations_params)
    df = transform_metadata(df, stations_cols_wanted)
    logger.info(f"Retrieved {len(df):,} rows of bikeshare station metadata.")
    return df


@task
def get_bikeshare_trips_data(
    trips_data_glob_str: str,
    trips_nan_cols: List[str],
    trips_duplicated_cols: List[str],
) -> pd.DataFrame:
    """Retrieve and process bikeshare trips data."""
    logger = prefect.context.get("logger")
    df = bt.load_trips_data(trips_data_glob_str)
    df = process_trips_data(df, trips_nan_cols, trips_duplicated_cols)
    logger.info(f"Retrieved {len(df):,} rows of bikeshare trips data.")
    return df


@task
def get_city_cultural_hotspots_data(
    open_tor_data_url: str, ch_params: Dict[str, str]
) -> pd.DataFrame:
    """Retrieve cultural hotspots open dataset."""
    logger = prefect.context.get("logger")
    df = cpd.get_cultural_hotspots(open_tor_data_url, ch_params)
    logger.info(
        f"Retrieved {len(df):,} rows of citywide cultural hotspot data."
    )
    return df


@task
def get_city_points_of_interest_data(
    open_tor_data_url: str, poi_params: Dict[str, str]
) -> pd.DataFrame:
    """Retrieve points of interest open dataset."""
    logger = prefect.context.get("logger")
    df = cpd.get_poi_data(open_tor_data_url, poi_params)
    logger.info(
        f"Retrieved {len(df):,} rows of citywide points-of-interest data."
    )
    return df


@task
def get_city_neighbourhood_boundary_data(
    open_tor_data_url: str,
    neigh_boundary_params: Dict[str, str],
    neigh_cols_to_show: List[str],
) -> pd.DataFrame:
    """Retrieve city neighbourhood boundary open dataset."""
    logger = prefect.context.get("logger")
    gdf = cpd.get_neighbourhood_boundary_land_area_data(
        open_tor_data_url, neigh_boundary_params, neigh_cols_to_show
    )
    logger.info(
        f"Retrieved {len(gdf):,} rows of city neighbourhood boundary data."
    )
    return gdf


@task
def get_city_public_transit_locations_data(
    open_tor_data_url: str, pt_params: Dict[str, str]
) -> pd.DataFrame:
    """Retrieve city public transit locations open dataset."""
    logger = prefect.context.get("logger")
    df = cpd.get_public_transit_locations(open_tor_data_url, pt_params)
    logger.info(
        f"Retrieved {len(df):,} rows of city public transit location data."
    )
    return df


@task
def get_city_college_university_locations_data() -> pd.DataFrame:
    """Retrieve city college and university location data."""
    logger = prefect.context.get("logger")
    df = cpd.get_coll_univ_locations()
    logger.info(
        f"Retrieved {len(df):,} rows of city college-univ location data."
    )
    return df


@task
def get_neighbourhood_profile_data(
    open_tor_data_url: str, neigh_profile_params: Dict[str, str]
) -> pd.DataFrame:
    """Retrieve city neighbourhood profiles open dataset."""
    logger = prefect.context.get("logger")
    df = cn.get_neighbourhood_profile_data(
        open_tor_data_url, neigh_profile_params
    )
    logger.info(
        f"Retrieved {len(df):,} rows of city neighbourhood profile data."
    )
    return df


@task(nout=6)
def aggregate_data(
    gdf: pd.DataFrame,
    df_poi: pd.DataFrame,
    dfch_essentials: pd.DataFrame,
    df_coll_univ: pd.DataFrame,
    df_pt_slice: pd.DataFrame,
    df_neigh_demog: pd.DataFrame,
    df_stations: pd.DataFrame,
) -> Tuple[pd.DataFrame]:
    """Combine neighbourhood stats and hourly bikeshare trips."""
    geo_cols = ["AREA_NAME", "geometry", "Shape__Area"]
    logger = prefect.context.get("logger")
    # Add neighbourhood to points-of-interest data
    df_poi_new = pa.check_io(out=ad.poi_new_schema)(
        cn.get_data_with_neighbourhood
    )(
        gdf[geo_cols],
        df_poi.rename(
            columns={
                "POI_LATITUDE": "lat",
                "POI_LONGITUDE": "lon",
            }
        )[["ID", "NAME", "lat", "lon"]],
        "lat",
        "lon",
        "ID",
        use_prefect=True,
    )
    logger.info("Added neighbourhood to city points-of-interest data")

    # Add neighbourhood to cultural hotspots data
    dfch_essentials_new = pa.check_output(ad.ch_essentials_new_schema)(
        cn.get_data_with_neighbourhood
    )(
        gdf[geo_cols],
        dfch_essentials.rename(
            columns={
                "POI_LATITUDE": "lat",
                "POI_LONGITUDE": "lon",
            }
        )[["ID", "NAME", "lat", "lon"]],
        "lat",
        "lon",
        "ID",
        use_prefect=True,
    )
    logger.info("Added neighbourhood to city cultural hotspots data")

    # Add neighbourhood to college and university location data
    df_coll_univ_new = pa.check_output(ad.coll_univ_schema_new)(
        cn.get_data_with_neighbourhood
    )(
        gdf[geo_cols],
        df_coll_univ,
        "lat",
        "lon",
        "institution_id",
        use_prefect=True,
    )
    logger.info(
        "Added neighbourhood to city college and university locations data"
    )

    # Add neighbourhood to public transit locations data
    df_pt_slice_new = pa.check_output(ad.pub_trans_locations_schema_new)(
        cn.get_data_with_neighbourhood
    )(
        gdf[geo_cols],
        df_pt_slice,
        "lat",
        "lon",
        "stop_id",
        use_prefect=True,
    )
    logger.info("Added neighbourhood to city public transit locations data")

    # Aggregate above neighbourhood stats and combine with demographics
    df_neigh_stats = ad.combine_neigh_stats(
        gdf,
        df_pt_slice_new,
        df_coll_univ_new,
        dfch_essentials_new,
        df_poi_new,
        df_neigh_demog,
    )
    logger.info("Aggregated statistics per city neighbourhood")

    # Add neighbourhood to stations locations
    df_stations_new = pa.check_output(ad.stations_schema_merged)(
        cn.get_data_with_neighbourhood
    )(
        gdf[geo_cols],
        df_stations,
        "lat",
        "lon",
        "station_id",
        use_prefect=True,
    )
    logger.info("Added stations to bikeshare station metadata")

    # Add stations to combined+aggregated neighbourhood stats
    df_stations_new = ad.combine_stations_metadata_neighbourhood(
        df_stations_new, df_neigh_stats
    )
    logger.info(
        "Combined stats and bikeshare station metadata per neighbourhood"
    )
    return [
        df_poi_new,
        dfch_essentials_new,
        df_coll_univ_new,
        df_pt_slice_new,
        df_neigh_stats,
        df_stations_new,
    ]


@task
def combine_trips_neighbourhood_data(
    df: pd.DataFrame, cols: List[str], df_stations_new: pd.DataFrame
) -> pd.DataFrame:
    """Combine hourly ridership and neighbourhood aggregated stats."""
    logger = prefect.context.get("logger")
    df_hour_by_station_merged = ad.combine_hourly_trips_per_station(
        df, cols, df_stations_new
    )
    logger.info(
        "Created aggregation of hourly trips per station with "
        "neighbourhood stats"
    )
    return df_hour_by_station_merged


@task
def export_aggregated_data_multiple_csvs(
    df: pd.DataFrame,
    cols_to_export: List[str],
    nrows_per_staged_csv_file: int,
) -> None:
    """Split a single DataFrame into multiple CSV files."""
    pa.check_input(ad.hourly_trips_by_station_merged_schema)(
        export_df_to_multiple_csv_files
    )(
        df,
        cols_to_export,
        "local_stage",
        nrows_per_staged_csv_file,
        use_prefect=True,
    )
