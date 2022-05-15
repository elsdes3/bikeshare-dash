#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Aggregate datasets at neighbourhood or station level."""

# pylint: disable=invalid-name,dangerous-default-value


from typing import List

import geopandas as gpd
import pandas as pd
import pandera as pa

from src.city_neighbourhoods import neigh_demog_schema
from src.city_pub_data import gdf_schema

stations_schema_merged = pa.DataFrameSchema(
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
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "Shape__Area": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
poi_new_schema = pa.DataFrameSchema(
    columns={
        "ID": pa.Column(pa.Int, unique=True),
        "NAME": pa.Column(pd.StringDtype(), unique=True),
        "lat": pa.Column(pa.Float, unique=False),
        "lon": pa.Column(pa.Float, unique=False),
        "Shape__Area": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
ch_essentials_new_schema = pa.DataFrameSchema(
    columns={
        "ID": pa.Column(pa.Int),
        "NAME": pa.Column(pd.StringDtype()),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "Shape__Area": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
neigh_stats_schema = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "neigh_shape_area": pa.Column(pa.Float64),
        "neigh_shape_length": pa.Column(pa.Float64),
        "geometry": pa.Column("geometry"),
        "neigh_area_latitude": pa.Column(pa.Float64),
        "neigh_transit_stops": pa.Column(pa.Int),
        "neigh_colleges_univs": pa.Column(pa.Int),
        "neigh_cultural_attractions": pa.Column(pa.Int),
        "neigh_places_of_interest": pa.Column(pa.Int),
        # only Prefect needs the following 3 to be nullable
        "neigh_pop_2016": pa.Column(pa.Float64, nullable=True),
        "neigh_youth_15_24": pa.Column(pa.Float64, nullable=True),
        "neigh_work_age_25_54": pa.Column(pa.Float64, nullable=True),
    },
    index=pa.Index(pa.Int),
)
neigh_stats_schema_v2 = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "neigh_shape_area": pa.Column(pa.Float64),
        "geometry": pa.Column("geometry"),
        "neigh_area_latitude": pa.Column(pa.Float64),
        "neigh_colleges_univs": pa.Column(pa.Int),
    },
    index=pa.Index(pa.Int),
)

coll_univ_schema_new = pa.DataFrameSchema(
    columns={
        "institution_id": pa.Column(pa.Int),
        "institution_name": pa.Column(pd.StringDtype()),
        "lat": pa.Column(pa.Float64),
        "lon": pa.Column(pa.Float64),
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "Shape__Area": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
stations_new_schema = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "STATION_ID": pa.Column(pa.Int),
        "NAME": pa.Column(pd.StringDtype()),
        "PHYSICAL_CONFIGURATION": pa.Column(pd.StringDtype()),
        "LAT": pa.Column(pa.Float64),
        "LON": pa.Column(pa.Float64),
        "ALTITUDE": pa.Column(pa.Float64, nullable=True),
        "ADDRESS": pa.Column(pd.StringDtype()),
        "CAPACITY": pa.Column(pa.Int),
        "PHYSICALKEY": pa.Column(pa.Int),
        "TRANSITCARD": pa.Column(pa.Int),
        "CREDITCARD": pa.Column(pa.Int),
        "PHONE": pa.Column(pa.Int),
        "SHAPE_AREA": pa.Column(pa.Float64),
        "NEIGH_SHAPE_AREA": pa.Column(pa.Float64),
        "NEIGH_SHAPE_LENGTH": pa.Column(pa.Float64),
        "GEOMETRY": pa.Column("geometry"),
        "NEIGH_AREA_LATITUDE": pa.Column(pa.Float64),
        "NEIGH_AREA_LONGITUDE": pa.Column(pa.Float64),
        "NEIGH_TRANSIT_STOPS": pa.Column(pa.Int),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pa.Int),
        "NEIGH_CULTURAL_ATTRACTIONS": pa.Column(pa.Int),
        "NEIGH_PLACES_OF_INTEREST": pa.Column(pa.Int),
        # only Prefect needs the following 3 to be nullable
        "NEIGH_POP_2016": pa.Column(pa.Float64, nullable=True),
        "NEIGH_YOUTH_15_24": pa.Column(pa.Float64, nullable=True),
        "NEIGH_WORK_AGE_25_54": pa.Column(pa.Float64, nullable=True),
    },
    index=pa.Index(pa.Int),
)
stations_new_schema_v2 = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "STATION_ID": pa.Column(pa.Int),
        "NAME": pa.Column(pd.StringDtype()),
        "PHYSICAL_CONFIGURATION": pa.Column(pd.StringDtype()),
        # "LAT": pa.Column(pa.Float64),
        # "LON": pa.Column(pa.Float64),
        # "ALTITUDE": pa.Column(pa.Float64, nullable=True),
        # "ADDRESS": pa.Column(pd.StringDtype()),
        "CAPACITY": pa.Column(pa.Int),
        "PHYSICALKEY": pa.Column(pa.Int),
        "TRANSITCARD": pa.Column(pa.Int),
        "CREDITCARD": pa.Column(pa.Int),
        "PHONE": pa.Column(pa.Int),
        # "SHAPE_AREA": pa.Column(pa.Float64),
        "NEIGH_SHAPE_AREA": pa.Column(pa.Float64),
        "GEOMETRY": pa.Column("geometry"),
        # "NEIGH_AREA_LATITUDE": pa.Column(pa.Float64),
        # "NEIGH_AREA_LONGITUDE": pa.Column(pa.Float64),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pa.Int),
    },
    index=pa.Index(pa.Int),
)
pub_trans_locations_schema_new = pa.DataFrameSchema(
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
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "Shape__Area": pa.Column(pa.Float64),
    },
    index=pa.Index(pa.Int),
)
aggregated_station_hourly_trips_schema = pa.DataFrameSchema(
    columns={
        "STATION_NAME": pa.Column(pd.StringDtype()),
        "YEAR": pa.Column(pa.Int),
        "MONTH": pa.Column(pa.Int),
        "DAY": pa.Column(pa.Int),
        "HOUR": pa.Column(pa.Int),
        "USER_TYPE": pa.Column(pd.StringDtype()),
        "NUM_TRIPS": pa.Column(pa.Int),
        "DURATION_MIN": pa.Column(pa.Int),
        "DURATION_MEDIAN": pa.Column(pa.Float),
        "DURATION_MEAN": pa.Column(pa.Float),
        "DURATION_MAX": pa.Column(pa.Int),
        "STATION_TYPE": pa.Column(pd.StringDtype()),
    },
    index=pa.Index(pa.Int),
)
hourly_trips_by_station_merged_schema = pa.DataFrameSchema(
    columns={
        "STATION_NAME": pa.Column(pd.StringDtype()),
        "YEAR": pa.Column(pa.Int),
        "MONTH": pa.Column(pa.Int),
        "DAY": pa.Column(pa.Int),
        "HOUR": pa.Column(pa.Int),
        "USER_TYPE": pa.Column(pd.StringDtype()),
        "NUM_TRIPS": pa.Column(pa.Int),
        "DURATION_MIN": pa.Column(pa.Int),
        "DURATION_MEDIAN": pa.Column(pa.Float),
        "DURATION_MEAN": pa.Column(pa.Float),
        "DURATION_MAX": pa.Column(pa.Int),
        "STATION_TYPE": pa.Column(pd.StringDtype()),
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "STATION_ID": pa.Column(pa.Int),
        "PHYSICAL_CONFIGURATION": pa.Column(pd.StringDtype()),
        "LAT": pa.Column(pa.Float64),
        "LON": pa.Column(pa.Float64),
        "ALTITUDE": pa.Column(pa.Float64, nullable=True),
        "ADDRESS": pa.Column(pd.StringDtype()),
        "CAPACITY": pa.Column(pa.Int),
        "PHYSICALKEY": pa.Column(pa.Int),
        "TRANSITCARD": pa.Column(pa.Int),
        "CREDITCARD": pa.Column(pa.Int),
        "PHONE": pa.Column(pa.Int),
        "SHAPE_AREA": pa.Column(pa.Float64),
        "NEIGH_SHAPE_AREA": pa.Column(pa.Float64),
        "NEIGH_SHAPE_LENGTH": pa.Column(pa.Float64),
        "NEIGH_AREA_LATITUDE": pa.Column(pa.Float64),
        "NEIGH_AREA_LONGITUDE": pa.Column(pa.Float64),
        "NEIGH_TRANSIT_STOPS": pa.Column(pa.Int),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pa.Int),
        "NEIGH_CULTURAL_ATTRACTIONS": pa.Column(pa.Int),
        "NEIGH_PLACES_OF_INTEREST": pa.Column(pa.Int),
        # only Prefect needs the following 3 to be nullable
        "NEIGH_POP_2016": pa.Column(pa.Float64, nullable=True),
        "NEIGH_YOUTH_15_24": pa.Column(pa.Float64, nullable=True),
        "NEIGH_WORK_AGE_25_54": pa.Column(pa.Float64, nullable=True),
    },
    index=pa.Index(pa.Int),
)


@pa.check_output(stations_new_schema)
def combine_stations_metadata_neighbourhood(
    df: pd.DataFrame, df_neighbourhood_stats: pd.DataFrame
) -> pd.DataFrame:
    """Combine bikeshare stations metadata with neighbourhood stats data."""
    df_merged = (
        df.set_index("AREA_NAME")
        .merge(
            df_neighbourhood_stats.set_index("AREA_NAME"),
            left_index=True,
            right_index=True,
            how="left",
        )
        .reset_index()
        .rename(columns={"Shape__Area": "Shape_Area"})
        .astype({"AREA_NAME": pd.StringDtype()})
    )
    df_merged.columns = df_merged.columns.str.upper()
    return df_merged


@pa.check_output(stations_new_schema_v2)
def combine_stations_metadata_neighbourhood_v2(
    df: pd.DataFrame, df_neighbourhood_stats: pd.DataFrame
) -> pd.DataFrame:
    """Combine bikeshare stations metadata with neighbourhood stats data."""
    df_merged = (
        df.set_index("AREA_NAME")
        .merge(
            df_neighbourhood_stats.set_index("AREA_NAME"),
            left_index=True,
            right_index=True,
            how="left",
        )
        .reset_index()
        # .rename(columns={"Shape__Area": "Shape_Area"})
        .astype({"AREA_NAME": pd.StringDtype()})
        .drop(
            columns=[
                "Shape__Area",
                "lat",
                "lon",
                "altitude",
                "address",
                "neigh_area_latitude",
                "neigh_area_longitude",
            ]
        )
    )
    df_merged.columns = df_merged.columns.str.upper()
    return df_merged


@pa.check_output(aggregated_station_hourly_trips_schema)
def get_aggregated_station_hourly_trips(
    df: pd.DataFrame, cols: List, trip_point: str = "start"
) -> pd.DataFrame:
    """Get hourly ridreship per bikeshare station."""
    trip_point_cols = [f"{trip_point}_{c}" for c in cols] + ["USER_TYPE"]
    station_trips = df.groupby(trip_point_cols, as_index=False).agg(
        {"TRIP_ID": "count", "DURATION": ["min", "median", "mean", "max"]}
    )
    station_trips.columns = (
        cols
        + ["USER_TYPE"]
        + [
            "NUM_TRIPS",
            "DURATION_min",
            "DURATION_median",
            "DURATION_mean",
            "DURATION_max",
        ]
    )
    station_trips = (
        station_trips.assign(station_type=trip_point)
        .rename(columns={"TRIP_ID_count": "NUM_TRIPS"})
        .sort_values(by="NUM_TRIPS", ascending=False)
        .astype(
            {
                "station_type": pd.StringDtype(),
                "USER_TYPE": pd.StringDtype(),
                "STATION_NAME": pd.StringDtype(),
            }
        )
    )
    station_trips.columns = station_trips.columns.str.upper()
    return station_trips


@pa.check_io(
    df_stations_new=stations_new_schema,
    out=hourly_trips_by_station_merged_schema,
)
def combine_hourly_trips_per_station(
    df: pd.DataFrame, cols: List[str], df_stations_new: pd.DataFrame
) -> pd.DataFrame:
    """Combine hourly bikeshare ridership with station metadata."""
    renamer_dict = {"NAME": "STATION_NAME"}
    df_hour_by_station_merged = pd.concat(
        [
            get_aggregated_station_hourly_trips(
                df,
                cols,
                trip_point,
            )
            .merge(
                df_stations_new.rename(columns=renamer_dict).astype(
                    {
                        "STATION_ID": pd.Int64Dtype(),
                        "CAPACITY": pd.Int64Dtype(),
                        "PHYSICALKEY": pd.Int64Dtype(),
                        "TRANSITCARD": pd.Int64Dtype(),
                        "CREDITCARD": pd.Int64Dtype(),
                        "PHONE": pd.Int64Dtype(),
                        "NEIGH_TRANSIT_STOPS": pd.Int64Dtype(),
                        "NEIGH_COLLEGES_UNIVS": pd.Int64Dtype(),
                        "NEIGH_CULTURAL_ATTRACTIONS": pd.Int64Dtype(),
                        "NEIGH_PLACES_OF_INTEREST": pd.Int64Dtype(),
                    }
                ),
                on="STATION_NAME",
                how="left",
            )
            .dropna(subset=["CAPACITY"])
            for trip_point in ["START", "END"]
        ],
        ignore_index=True,
    )
    return df_hour_by_station_merged


@pa.check_io(
    gdf=gdf_schema,
    df_pt_slice_new=pub_trans_locations_schema_new,
    df_coll_univ_new=coll_univ_schema_new,
    dfch_essentials_new=ch_essentials_new_schema,
    df_poi_new=poi_new_schema,
    df_neigh_demog=neigh_demog_schema,
    out=neigh_stats_schema,
)
def combine_neigh_stats(
    gdf: gpd.geodataframe,
    df_pt_slice_new: pd.DataFrame,
    df_coll_univ_new: pd.DataFrame,
    dfch_essentials_new: pd.DataFrame,
    df_poi_new: pd.DataFrame,
    df_neigh_demog: pd.DataFrame,
) -> gpd.geodataframe:
    """Combine different neighbourhood-wise aggregated datasets."""
    df_neigh_stats = (
        (
            gdf.set_index("AREA_NAME")[
                [
                    "Shape__Area",
                    "Shape__Length",
                    "geometry",
                    # "CLASSIFICATION",
                    # "CLASSIFICATION_CODE",
                    "AREA_LATITUDE",
                    "AREA_LONGITUDE",
                ]
            ]
            .merge(
                df_pt_slice_new.groupby("AREA_NAME")["stop_id"]
                .count()
                .rename("transit_stops")
                .to_frame(),
                left_index=True,
                right_index=True,
                how="left",
            )
            .merge(
                df_coll_univ_new.groupby("AREA_NAME")["institution_id"]
                .count()
                .rename("colleges_univs")
                .to_frame(),
                left_index=True,
                right_index=True,
                how="left",
            )
            .merge(
                dfch_essentials_new.groupby("AREA_NAME")["ID"]
                .count()
                .rename("cultural_attractions")
                .to_frame(),
                left_index=True,
                right_index=True,
                how="left",
            )
            .merge(
                df_poi_new.groupby("AREA_NAME")["ID"]
                .count()
                .rename("places_of_interest")
                .to_frame(),
                left_index=True,
                right_index=True,
                how="left",
            )
            .fillna(0)
            .astype(
                {
                    k: int
                    for k in [
                        "transit_stops",
                        "colleges_univs",
                        "cultural_attractions",
                        "places_of_interest",
                    ]
                }
            )
            .merge(
                df_neigh_demog.set_index("AREA_NAME")[
                    [
                        "Population, 2016",
                        "Youth (15-24 years)",
                        "Working Age (25-54 years)",
                    ]
                ].rename(
                    columns={
                        "Population, 2016": "pop_2016",
                        "Youth (15-24 years)": "youth_15_24",
                        "Working Age (25-54 years)": "work_age_25_54",
                    }
                ),
                left_index=True,
                right_index=True,
                how="left",
            )
        )
        .add_prefix("neigh_")
        .rename(columns={"neigh_geometry": "geometry"})
    )
    df_neigh_stats.columns = df_neigh_stats.columns.str.lower().str.replace(
        "__", "_"
    )
    df_neigh_stats = df_neigh_stats.reset_index().astype(
        {"AREA_NAME": pd.StringDtype()}
    )
    for c in ["neigh_pop_2016", "neigh_youth_15_24", "neigh_work_age_25_54"]:
        df_neigh_stats[c] = df_neigh_stats[c].str.replace(",", "")
        df_neigh_stats[c] = df_neigh_stats[c].astype(float)
    return df_neigh_stats


@pa.check_io(
    gdf=gdf_schema,
    df_coll_univ_new=coll_univ_schema_new,
    out=neigh_stats_schema_v2,
)
def combine_neigh_stats_v2(
    gdf: gpd.geodataframe, df_coll_univ_new: pd.DataFrame
) -> gpd.geodataframe:
    """Combine different neighbourhood-wise aggregated datasets."""
    df_neigh_stats = (
        (
            gdf.set_index("AREA_NAME")[
                [
                    "Shape__Area",
                    # "Shape__Length",
                    "geometry",
                    # "CLASSIFICATION",
                    # "CLASSIFICATION_CODE",
                    "AREA_LATITUDE",
                    "AREA_LONGITUDE",
                ]
            ]
            .merge(
                df_coll_univ_new.groupby("AREA_NAME")["institution_id"]
                .count()
                .rename("colleges_univs")
                .to_frame(),
                left_index=True,
                right_index=True,
                how="left",
            )
            .fillna(0)
            .astype({k: int for k in ["colleges_univs"]})
        )
        .add_prefix("neigh_")
        .rename(columns={"neigh_geometry": "geometry"})
    )
    df_neigh_stats.columns = df_neigh_stats.columns.str.lower().str.replace(
        "__", "_"
    )
    df_neigh_stats = df_neigh_stats.reset_index().astype(
        {"AREA_NAME": pd.StringDtype()}
    )
    return df_neigh_stats
