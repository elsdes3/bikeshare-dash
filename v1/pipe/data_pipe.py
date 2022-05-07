#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Data pipeline to assemble dataset."""

# pylint: disable=invalid-name,redefined-outer-name
# pylint: disable=too-many-locals,too-many-arguments,unexpected-keyword-arg

import argparse
import os
from typing import Dict, List

from prefect import Flow

import src.data_pipe_db_utils as dpdu
import src.data_pipe_utils as dpu


def create_cloud_resources(
    trips_db_name: str, stations_db_name: str, trips_file_format_name: str
) -> Flow:
    """Create cloud resources."""
    with Flow("Data retrieval, combination and aggregation") as flow:
        create_dbs = dpdu.create_databases(
            [trips_db_name, stations_db_name],
            True,
        )
        create_ffs = dpdu.create_or_replace_file_format(
            trips_file_format_name,
            trips_db_name,
            True,
            upstream_tasks=[create_dbs],
        )
        dpdu.create_or_replace_stage(
            trips_file_format_name,
            trips_stage_name,
            trips_db_name,
            True,
            upstream_tasks=[create_ffs],
        )
        dpdu.create_db_tables(
            trips_db_name,
            True,
        )
    return flow


def get_add_data(
    open_tor_data_url: str,
    trips_data_glob_str: str,
    stations_params: Dict[str, str],
    stations_cols_wanted: List[str],
    neigh_profile_params: Dict[str, str],
    pt_params: Dict[str, str],
    poi_params: Dict[str, str],
    ch_params: Dict[str, str],
    neigh_boundary_params: Dict[str, str],
    neigh_cols_to_show: List[str],
    trips_nan_cols: List[str],
    trips_duplicated_cols: List[str],
    cols: List[str],
    cols_to_export: List[str],
    nrows_per_staged_csv_file: int,
    trips_db_name: str,
    stations_db_name: str,
    trips_stage_name: str,
    trips_table_name: str,
    station_stats_table_name: str,
) -> Flow:
    """Get data and add to database."""
    with Flow("Data retrieval, combination and aggregation") as flow:
        df_stations = dpu.get_bikeshare_stations_metadata(
            open_tor_data_url,
            stations_params,
            stations_cols_wanted,
        )
        df = dpu.get_bikeshare_trips_data(
            trips_data_glob_str,
            trips_nan_cols,
            trips_duplicated_cols,
        )
        dfch_essentials = dpu.get_city_cultural_hotspots_data(
            open_tor_data_url, ch_params
        )
        params = dict(
            open_tor_data_url=open_tor_data_url, poi_params=poi_params
        )
        df_poi = dpu.get_city_points_of_interest_data(**params)
        gdf = dpu.get_city_neighbourhood_boundary_data(
            open_tor_data_url,
            neigh_boundary_params,
            neigh_cols_to_show,
        )
        df_pt_slice = dpu.get_city_public_transit_locations_data(
            open_tor_data_url, pt_params
        )
        df_coll_univ = dpu.get_city_college_university_locations_data()
        df_neigh_demog = dpu.get_neighbourhood_profile_data(
            open_tor_data_url, neigh_profile_params
        )

        _, _, _, _, _, df_stations_new = dpu.aggregate_data(
            gdf,
            df_poi,
            dfch_essentials,
            df_coll_univ,
            df_pt_slice,
            df_neigh_demog,
            df_stations,
        )

        df_hour_by_station_merged = dpu.combine_trips_neighbourhood_data(
            df, cols, df_stations_new
        )

        agg_data_csvs = dpu.export_aggregated_data_multiple_csvs(
            df_hour_by_station_merged,
            cols_to_export,
            nrows_per_staged_csv_file,
        )

        dpdu.add_gzip_compressed_csv_files_to_stage(
            trips_stage_name,
            trips_db_name,
            "data/processed/local_stage_*.csv.gz",
            True,
            upstream_tasks=[agg_data_csvs],
        )
        dpdu.add_data_to_trips_table(
            trips_table_name,
            trips_stage_name,
            trips_db_name,
            True,
        )
        dpdu.add_dataframe_to_stations_table(
            df_stations_new,
            station_stats_table_name,
            stations_db_name,
            True,
        )
    return flow


def delete_resources(
    trips_table_name: str,
    station_stats_table_name: str,
    trips_stage_name: str,
    trips_file_format_name: str,
    trips_db_name: str,
    stations_db_name: str,
) -> Flow:
    """Delete resources and local data."""
    with Flow("Data and Resource Deletion") as flow:
        remove_snow = dpdu.delete_snowflake_resources(
            trips_table_name,
            station_stats_table_name,
            trips_stage_name,
            trips_file_format_name,
            trips_db_name,
            stations_db_name,
            True,
        )
        dpdu.delete_local_data_files("data", upstream_tasks=[remove_snow])
    return flow


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nrows-per-staged-csv-file",
        type=int,
        dest="nrows_per_staged_csv_file",
        default=380_000,
        help="number of rows per CSV file created from final data",
    )
    parser.add_argument(
        "--action",
        type=str,
        dest="action",
        default="create",
        help="whether to create or destroy resources",
    )
    args = parser.parse_args()

    open_tor_data_url = (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    )

    trips_data_glob_str = "data/raw/*.csv"

    stations_params = {"id": "2b44db0d-eea9-442d-b038-79335368ad5a"}
    stations_cols_wanted = [
        "station_id",
        "name",
        "physical_configuration",
        "lat",
        "lon",
        "altitude",
        "address",
        "capacity",
        "physicalkey",
        "transitcard",
        "creditcard",
        "phone",
    ]
    neigh_profile_params = {"id": "6e19a90f-971c-46b3-852c-0c48c436d1fc"}
    pt_params = {"id": "7795b45e-e65a-4465-81fc-c36b9dfff169"}
    poi_params = {"id": "965247c0-c72e-49b4-bb1a-879cf98e1a32"}
    ch_params = {"id": "c7be2ee7-d317-4a28-8cbe-bff1ce116b46"}
    neigh_boundary_params = {"id": "4def3f65-2a65-4a4f-83c4-b2a4aed72d46"}

    neigh_cols_to_show = [
        "AREA_ID",
        "AREA_SHORT_CODE",
        "AREA_LONG_CODE",
        "AREA_NAME",
        "Shape__Area",
        "Shape__Length",
        "LATITUDE",
        "AREA_LATITUDE",
        "LONGITUDE",
        "AREA_LONGITUDE",
        "geometry",
    ]
    trips_nan_cols = [
        "START_STATION_ID",
        "END_STATION_ID",
        "START_STATION_NAME",
        "END_STATION_NAME",
    ]
    trips_duplicated_cols = ["TRIP_ID", "START_TIME", "END_TIME"]

    cols = ["STATION_NAME", "year", "month", "day", "hour"]

    # Exporting to staged CSV files
    cols_to_export = [
        "STATION_NAME",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "USER_TYPE",
        "NUM_TRIPS",
        "DURATION_MEAN",
        "AREA_NAME",
        "PHYSICAL_CONFIGURATION",
        "CAPACITY",
        "PHYSICALKEY",
        "TRANSITCARD",
        "CREDITCARD",
        "PHONE",
        "NEIGH_TRANSIT_STOPS",
        "NEIGH_COLLEGES_UNIVS",
        "NEIGH_CULTURAL_ATTRACTIONS",
        "NEIGH_PLACES_OF_INTEREST",
    ]

    # Database administration
    trips_db_name = os.getenv("DB_NAME")
    stations_db_name = "torbikestations"
    trips_stage_name = "bikes_stage"
    trips_file_format_name = "COMMASEP_ONEHEADROW"
    trips_table_name = "trips"
    station_stats_table_name = "station_stats"

    flow = create_cloud_resources(
        trips_db_name, stations_db_name, trips_file_format_name
    )
    flow.run()

    if args.action == "create":
        flow = get_add_data(
            open_tor_data_url,
            trips_data_glob_str,
            stations_params,
            stations_cols_wanted,
            neigh_profile_params,
            pt_params,
            poi_params,
            ch_params,
            neigh_boundary_params,
            neigh_cols_to_show,
            trips_nan_cols,
            trips_duplicated_cols,
            cols,
            cols_to_export,
            args.nrows_per_staged_csv_file,
            trips_db_name,
            stations_db_name,
            trips_stage_name,
            trips_table_name,
            station_stats_table_name,
        )
    else:
        flow = delete_resources(
            trips_table_name,
            station_stats_table_name,
            trips_stage_name,
            trips_file_format_name,
            trips_db_name,
            stations_db_name,
        )
    flow.run()
