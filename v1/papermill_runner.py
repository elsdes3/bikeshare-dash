#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Programmatic execution of notebooks."""

# pylint: disable=invalid-name

import argparse
import os
from datetime import datetime
from typing import Dict, List

import papermill as pm

PROJ_ROOT_DIR = os.getcwd()
data_dir = os.path.join(PROJ_ROOT_DIR, "data")
output_notebook_dir = os.path.join(PROJ_ROOT_DIR, "executed_notebooks")

raw_data_path = os.path.join(data_dir, "raw")

zero_dict_nb_name = "0_get_bikeshare_data.ipynb"
zero_dict_v2_nb_name = "0_get_bikeshare_data_v2.ipynb"
one_dict_nb_name = "1_transform_bikeshare_data.ipynb"
two_dict_nb_name = "2_delete_data.ipynb"
two_dict_v2_nb_name = "2_delete_data_v2.ipynb"

zero_dict = dict(
    url=(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    ),
    params={"id": "7e876c24-177c-4605-9cef-e50dd74c617f"},
    about_params={"id": "2b44db0d-eea9-442d-b038-79335368ad5a"},
    path_to_sql_cfg="../sql.ini",
    table_name="ridership",
    n_rows_to_append_to_db=10_000,
    dtypes_dict={
        "Trip Duration": "int",
        "Start Station Id": "int",
        "End Station Id": "float",
        "Bike Id": "float",
    },
    geo_cols=["AREA_NAME", "geometry", "Shape__Area"],
)
zero_v2_dict = dict(
    url=(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    ),
    trips_params={"id": "7e876c24-177c-4605-9cef-e50dd74c617f"},
    years_wanted={2021: list(range(1, 12 + 1))},
    about_params={"id": "2b44db0d-eea9-442d-b038-79335368ad5a"},
    stations_cols_wanted=[
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
    ],
    neigh_cols_to_show=[
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
    ],
    date_cols=["Start Time", "End Time"],
    nan_cols=[
        "START_STATION_ID",
        "END_STATION_ID",
        "START_STATION_NAME",
        "END_STATION_NAME",
    ],
    duplicated_cols=["TRIP_ID", "START_TIME", "END_TIME"],
    stations_db_name="torbikestations",
    trips_table_name="trips",
    station_stats_table_name="station_stats",
    trips_stage_name="bikes_stage",
    trips_file_format_name="COMMASEP_ONEHEADROW",
    geo_cols=["AREA_NAME", "geometry", "Shape__Area"],
    cols_to_export=[
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
    ],
    nrows_per_staged_csv_file=350_000,
)
one_dict = dict(
    table_name="ridership",
    path_to_sql_cfg="../sql.ini",
    neigh_geo_url=(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    ),
    neigh_params={"id": "4def3f65-2a65-4a4f-83c4-b2a4aed72d46"},
)
two_dict = dict(table_name="ridership", path_to_sql_cfg="../sql.ini")
two_v2_dict = dict(
    stations_db_name="torbikestations",
    trips_table_name="trips",
    station_stats_table_name="station_stats",
    trips_stage_name="bikes_stage",
    trips_file_format_name="COMMASEP_ONEHEADROW",
)


def papermill_run_notebook(
    nb_dict: Dict, output_notebook_directory: str = "executed_notebooks"
) -> None:
    """Execute notebook with papermill"""
    for notebook, nb_params in nb_dict.items():
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_nb = os.path.basename(notebook).replace(
            ".ipynb", f"-{now}.ipynb"
        )
        print(
            f"\nInput notebook path: {notebook}",
            f"Output notebook path: {output_notebook_directory}/{output_nb} ",
            sep="\n",
        )
        for key, val in nb_params.items():
            print(key, val, sep=": ")
        pm.execute_notebook(
            input_path=notebook,
            output_path=f"{output_notebook_directory}/{output_nb}",
            parameters=nb_params,
        )


def run_notebooks(
    notebooks_list: List, output_notebook_directory: str = "executed_notebooks"
) -> None:
    """Execute notebooks from CLI.
    Parameters
    ----------
    nb_dict : List
        list of notebooks to be executed
    Usage
    -----
    > import os
    > PROJ_ROOT_DIR = os.path.abspath(os.getcwd())
    > one_dict_nb_name = "a.ipynb
    > one_dict = {"a": 1}
    > run_notebook(
          notebook_list=[
              {os.path.join(PROJ_ROOT_DIR, one_dict_nb_name): one_dict}
          ]
      )
    """
    for nb in notebooks_list:
        papermill_run_notebook(
            nb_dict=nb, output_notebook_directory=output_notebook_directory
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ci-run",
        type=str,
        dest="ci_run",
        default="yes",
        help="whether to run CI build",
    )
    parser.add_argument(
        "--action",
        type=str,
        dest="action",
        default="create",
        help="whether to create or destroy resources",
    )
    args = parser.parse_args()

    zero_v2_dict.update({"ci_run": args.ci_run})
    two_v2_dict.update({"ci_run": args.ci_run})

    if args.action == "create":
        nb_dict_list = [zero_v2_dict]
        nb_name_list = [zero_dict_v2_nb_name]
    else:
        nb_dict_list = [two_v2_dict]
        nb_name_list = [two_dict_v2_nb_name]
    notebook_list = [
        {os.path.join(PROJ_ROOT_DIR, nb_name): nb_dict}
        for nb_dict, nb_name in zip(nb_dict_list, nb_name_list)
    ]

    run_notebooks(
        notebooks_list=notebook_list,
        output_notebook_directory=output_notebook_dir,
    )
