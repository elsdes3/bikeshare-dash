#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Aggregate datasets at neighbourhood or station level."""

# pylint: disable=invalid-name,dangerous-default-value


import geopandas as gpd
import pandas as pd
import pandera as pa

from src.city_pub_data import gdf_schema
from src.process_trips import trips_schema_processed_v2
from src.utils import log_prefect, save_data_to_parquet_file

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
neigh_stats_schema_v2 = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "neigh_shape_area": pa.Column(pa.Float64),
        "geometry": pa.Column("geometry"),
        "neigh_area_latitude": pa.Column(pa.Float64),
        "neigh_area_longitude": pa.Column(pa.Float64),
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
stations_new_schema_v2 = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "STATION_ID": pa.Column(pa.Int),
        "NAME": pa.Column(pd.StringDtype()),
        "PHYSICAL_CONFIGURATION": pa.Column(pd.StringDtype()),
        "CAPACITY": pa.Column(pa.Int),
        "PHYSICALKEY": pa.Column(pa.Int),
        "TRANSITCARD": pa.Column(pa.Int),
        "CREDITCARD": pa.Column(pa.Int),
        "PHONE": pa.Column(pa.Int),
        "NEIGH_SHAPE_AREA": pa.Column(pa.Float64),
        "GEOMETRY": pa.Column("geometry"),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pa.Int),
    },
    index=pa.Index(pa.Int),
)
merge_trips_neighbourhood_schema = pa.DataFrameSchema(
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
        "START_year": pa.Column(pa.Int),
        "START_month": pa.Column(pa.Int),
        "START_weekday": pa.Column(pd.StringDtype()),
        "START_hour": pa.Column(pa.Int),
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "STATION_ID": pa.Column(pa.Int),
        "PHYSICAL_CONFIGURATION": pa.Column(pd.StringDtype()),
        "CAPACITY": pa.Column(pd.Int64Dtype()),
        "PHYSICALKEY": pa.Column(pd.Int64Dtype()),
        "TRANSITCARD": pa.Column(pd.Int64Dtype()),
        "CREDITCARD": pa.Column(pd.Int64Dtype()),
        "PHONE": pa.Column(pd.Int64Dtype()),
        "GEOMETRY": pa.Column("geometry"),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pd.Int64Dtype()),
    },
    index=pa.Index(pa.Int),
)
agg_schema = pa.DataFrameSchema(
    columns={
        "AREA_NAME": pa.Column(pd.StringDtype()),
        "USER_TYPE": pa.Column(
            pd.StringDtype(),
            checks=[
                pa.Check(
                    lambda s: s.isin(["Annual Member", "Casual Member"]),
                )
            ],
        ),
        "START_year": pa.Column(pa.Int),
        "START_month": pa.Column(pa.Int),
        "START_weekday": pa.Column(pd.StringDtype()),
        "START_hour": pa.Column(pa.Int),
        "TRIP_DURATION": pa.Column(pa.Int),
        "NUM_STATIONS": pa.Column(pa.Int),
        "NUM_TRIPS": pa.Column(pa.Int),
        "NUM_DOCKS": pa.Column(pd.Int64Dtype()),
        "NUM_TRIPS_PHYSICALKEY": pa.Column(pd.Int64Dtype()),
        "NUM_TRIPS_TRANSITCARD": pa.Column(pd.Int64Dtype()),
        "NUM_TRIPS_CREDITCARD": pa.Column(pd.Int64Dtype()),
        "NUM_TRIPS_PHONE": pa.Column(pd.Int64Dtype()),
        # "GEOMETRY": pa.Column("geometry"),
        "NEIGH_COLLEGES_UNIVS": pa.Column(pd.Int64Dtype()),
        "zip_file": pa.Column(pd.StringDtype()),
        "csv_file": pa.Column(pd.StringDtype()),
        "downloaded_file": pa.Column(pd.BooleanDtype()),
        "last_modified_timestamp": pa.Column(
            pd.DatetimeTZDtype(tz="America/Toronto"), nullable=True
        ),
    },
    index=pa.Index(pa.Int),
)


@pa.check_io(
    df=stations_schema_merged,
    df_neighbourhood_stats=neigh_stats_schema_v2,
    out=stations_new_schema_v2,
)
def combine_stations_metadata_neighbourhood_v2(
    df: pd.DataFrame,
    df_neighbourhood_stats: pd.DataFrame,
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Combine bikeshare stations metadata with neighbourhood stats data."""
    log_prefect(
        "Combining metadata and neighbourhood stats data...", True, use_prefect
    )
    df_merged = (
        df.set_index("AREA_NAME")
        .merge(
            df_neighbourhood_stats.set_index("AREA_NAME"),
            left_index=True,
            right_index=True,
            how="left",
        )
        .reset_index()
        .astype({"AREA_NAME": pd.StringDtype()})
        .drop(columns=["neigh_area_latitude", "neigh_area_longitude"])
    )
    df_merged.columns = df_merged.columns.str.upper()
    log_prefect("Done combining data.", False, use_prefect)
    return df_merged


@pa.check_io(
    gdf=gdf_schema,
    df_coll_univ_new=coll_univ_schema_new,
    out=neigh_stats_schema_v2,
)
def combine_neigh_stats_v2(
    gdf: gpd.geodataframe,
    df_coll_univ_new: pd.DataFrame,
    use_prefect: bool = False,
) -> gpd.geodataframe:
    """Combine different neighbourhood-wise aggregated datasets."""
    log_prefect(
        "Creating neighbourhood aggregations of stats...", True, use_prefect
    )
    df_neigh_stats = (
        (
            gdf.set_index("AREA_NAME")[
                ["Shape__Area", "geometry", "AREA_LATITUDE", "AREA_LONGITUDE"]
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
    assert type(df_neigh_stats).__name__ == "GeoDataFrame"
    log_prefect("Done creating.", False, use_prefect)
    return df_neigh_stats


@pa.check_io(
    df_trips=trips_schema_processed_v2,
    df_stations_stats=stations_new_schema_v2,
    out=merge_trips_neighbourhood_schema,
)
def merge_trips_neighbourhood_stats(
    df_trips: pd.DataFrame,
    df_stations_stats: pd.DataFrame,
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Merge bikeshare trips and aggregated neighbourhood statistics data."""
    log_prefect(
        "Merging ridership and neighbourhood stats data...", True, use_prefect
    )
    df_merged = df_trips.merge(
        df_stations_stats.rename(columns={"NAME": "START_STATION_NAME"}),
        on="START_STATION_NAME",
        how="inner",
    ).astype(
        {
            "STATION_ID": pd.Int64Dtype(),
            "CAPACITY": pd.Int64Dtype(),
            "PHYSICALKEY": pd.Int64Dtype(),
            "TRANSITCARD": pd.Int64Dtype(),
            "CREDITCARD": pd.Int64Dtype(),
            "PHONE": pd.Int64Dtype(),
            "NEIGH_COLLEGES_UNIVS": pd.Int64Dtype(),
        }
    )
    log_prefect("Done merging.", False, use_prefect)
    return df_merged


@pa.check_io(data_merged=merge_trips_neighbourhood_schema, out=agg_schema)
def aggregate_merged_data(
    data_merged: pd.DataFrame,
    zip_file: str,
    downloaded_file: bool,
    csv_file: str,
    last_mod: pd.Timestamp,
    use_prefect: bool = False,
) -> pd.DataFrame:
    """Aggregated merged ridership and station metadata."""
    log_prefect("Aggregating merged data...", True, use_prefect)
    data_agg = (
        data_merged.groupby(
            [
                "AREA_NAME",
                "USER_TYPE",
                "START_year",
                "START_month",
                "START_weekday",
                "START_hour",
            ],
            as_index=False,
        )
        .agg(
            {
                "TRIP__DURATION": "sum",
                "START_STATION_NAME": "count",
                "START_TIME": "count",
                "CAPACITY": "sum",
                "PHYSICALKEY": "count",
                "TRANSITCARD": "count",
                "CREDITCARD": "count",
                "PHONE": "count",
                "NEIGH_COLLEGES_UNIVS": "sum",
                # "GEOMETRY": "first",
            }
        )
        .rename(
            columns={
                "TRIP__DURATION": "TRIP_DURATION",
                "START_STATION_NAME": "NUM_STATIONS",
                "START_TIME": "NUM_TRIPS",
                "CAPACITY": "NUM_DOCKS",
                "PHYSICALKEY": "NUM_TRIPS_PHYSICALKEY",
                "TRANSITCARD": "NUM_TRIPS_TRANSITCARD",
                "CREDITCARD": "NUM_TRIPS_CREDITCARD",
                "PHONE": "NUM_TRIPS_PHONE",
            }
        )
        .assign(zip_file=zip_file)
        .assign(csv_file=csv_file)
        .assign(downloaded_file=downloaded_file)
        .assign(last_modified_timestamp=last_mod)
        .astype(
            {
                "AREA_NAME": pd.StringDtype(),
                "USER_TYPE": pd.StringDtype(),
                "START_year": pd.Int64Dtype(),
                "START_weekday": pd.StringDtype(),
                "TRIP_DURATION": pd.Int64Dtype(),
                "csv_file": pd.StringDtype(),
                "zip_file": pd.StringDtype(),
                "downloaded_file": pd.BooleanDtype(),
            }
        )
    )
    log_prefect("Done aggregating.", False, use_prefect)
    return data_agg


@pa.check_io(data=agg_schema)
def update_parquet_file_data(
    data: pd.DataFrame, raw_data_filepath: str, updated_data_filepath: str
) -> None:
    """Update trips data in sections of parquet file, if outdated."""
    # Load current parquet file
    df_parquet = pd.read_parquet(raw_data_filepath)
    # Get the name of the zip file whhose contents must replace current
    # contents of the parquet file
    updated_zip_files = data["zip_file"].unique().to_numpy().tolist()
    # Get non-outdated contents of current parquet file based on name of
    # the zip file (get the zip files that do not contain outdated data)
    df_parquet_non_updated = df_parquet.query(
        "~zip_file.isin(@updated_zip_files)"
    )
    # Combine contents of current parquet file that are not outdated with
    # updated contents
    df_parquet_updated = pd.concat(
        [df_parquet_non_updated, data], ignore_index=True
    )
    # Export updated contents to parquet file
    save_data_to_parquet_file(df_parquet_updated, updated_data_filepath)
