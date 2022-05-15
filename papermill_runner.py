#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Programmatic execution of notebooks."""

# pylint: disable=invalid-name

import os
from datetime import datetime
from typing import Dict, List

import papermill as pm

PROJ_ROOT_DIR = os.getcwd()
data_dir = os.path.join(PROJ_ROOT_DIR, "data")
output_notebook_dir = os.path.join(PROJ_ROOT_DIR, "executed_notebooks")

raw_data_path = os.path.join(data_dir, "raw")

one_dict_nb_name = "01_get_data.ipynb"

one_dict = dict(
    url=(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    ),
    trips_params={"id": "7e876c24-177c-4605-9cef-e50dd74c617f"},
    years_wanted=[2021],
    neigh_boundary_params={"id": "4def3f65-2a65-4a4f-83c4-b2a4aed72d46"},
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
        "AREA_LATITUDE",
        "AREA_LONGITUDE",
        "geometry",
    ],
    date_cols=["Start Time", "End Time"],
    nan_cols=[
        "START_STATION_ID",
        "START_STATION_NAME",
    ],
    duplicated_cols=["TRIP_ID", "START_TIME"],
    geo_cols=["AREA_NAME", "geometry", "Shape__Area"],
    raw_data_dir="data/raw",
    processed_data_dir="data/processed",
    parquet_filename="agg_data.parquet.gzip",
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
    notebooks_list: List, output_nb_dir: str = "executed_notebooks"
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
            nb_dict=nb, output_notebook_directory=output_nb_dir
        )


if __name__ == "__main__":
    nb_dict_list = [one_dict]
    nb_name_list = [one_dict_nb_name]

    notebook_list = [
        {os.path.join(PROJ_ROOT_DIR, nb_name): nb_dict}
        for nb_dict, nb_name in zip(nb_dict_list, nb_name_list)
    ]

    run_notebooks(
        notebooks_list=notebook_list,
        output_nb_dir=output_notebook_dir,
    )
