#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Data pipeline database operations utilities."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=too-many-arguments,redefined-outer-name,too-many-locals

import os
import shutil
from glob import glob
from typing import List

import pandas as pd
import prefect
import snowflake.connector
from prefect import task
from snowflake.connector.pandas_tools import write_pandas


@task
def create_databases(db_names: List[str], use_prefect: bool = False) -> None:
    """Create databases if they does not exist."""
    logger = prefect.context.get("logger")
    snowflake_dict_no_db = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict_no_db)
    cur = conn.cursor()
    for db_name in db_names:
        _ = cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        create_db_str = f"Created database {db_name}"
        if use_prefect:
            logger.info(create_db_str)
        else:
            print(create_db_str)
    cur.close()
    conn.close()


@task
def create_or_replace_file_format(
    ff_name: str, db_name: str, use_prefect: bool = False
) -> None:
    """Create or replace file format for gzip-compressed CSV files."""
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    query = rf"""
            CREATE OR REPLACE FILE FORMAT {ff_name}
            TYPE = 'CSV'
            COMPRESSION = 'GZIP'
            FIELD_DELIMITER = ','
            RECORD_DELIMITER = '\n'
            SKIP_HEADER = 1
            TRIM_SPACE = FALSE
            ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            ESCAPE = 'NONE'
            DATE_FORMAT = 'AUTO'
            TIMESTAMP_FORMAT = 'AUTO'
            NULL_IF = ('\\N')
            """
    _ = cur.execute(query)
    if use_prefect:
        logger = prefect.context.get("logger")
        logger.info(f"Created or replaced file format {ff_name}")
    else:
        print(query.strip())
    cur.close()
    conn.close()


@task
def create_or_replace_stage(
    ff_name: str, stage_name: str, db_name: str, use_prefect: bool = False
) -> None:
    """Create or replace stage for gzip-compressed CSV files."""
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    query = f"""
            CREATE OR REPLACE STAGE {stage_name}
            FILE_FORMAT = {ff_name}
            """
    _ = cur.execute(query)
    if use_prefect:
        logger = prefect.context.get("logger")
        logger.info(f"Created or replaced stage {stage_name}")
    else:
        print(query.strip())
    cur.close()
    conn.close()


@task
def add_gzip_compressed_csv_files_to_stage(
    stage_name: str,
    db_name: str,
    glob_str: str = "data/processed/local_stage_*.csv.gz",
    use_prefect: bool = False,
) -> None:
    """Add gzip-compressed CSV files to stage."""
    if use_prefect:
        logger = prefect.context.get("logger")
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    for file in glob(glob_str):
        query = f"""
                PUT file://{file} @{stage_name}
                """
        _ = cur.execute(query)
        if use_prefect:
            logger.info(query.strip())
        else:
            print(query.strip())
    cur.close()
    conn.close()


@task
def create_db_tables(
    db_name: str,
    use_prefect: bool = False,
) -> None:
    """Create database tables."""
    if use_prefect:
        logger = prefect.context.get("logger")
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    query_1 = """
              CREATE OR REPLACE TABLE trips (
                  station_name VARCHAR(100),
                  year INT,
                  month INT,
                  day INT,
                  hour INT,
                  user_type VARCHAR(20),
                  num_trips INT,
                  duration_mean FLOAT,
                  station_type VARCHAR(10),
                  area_name TEXT,
                  physical_configuration TEXT,
                  capacity INT,
                  physicalkey INT,
                  transitcard INT,
                  creditcard INT,
                  phone INT,
                  neigh_transit_stops INT,
                  neigh_colleges_univs INT,
                  neigh_cultural_attractions INT,
                  neigh_places_of_interest INT
              )
              """
    query_2 = """
              CREATE OR REPLACE TABLE station_stats (
                  area_name string,
                  station_id integer,
                  name string,
                  physical_configuration string,
                  lat float,
                  lon float,
                  altitude float,
                  address string,
                  capacity integer,
                  physicalkey integer,
                  transitcard integer,
                  creditcard integer,
                  phone integer,
                  shape_area float,
                  neigh_shape_area float,
                  neigh_shape_length float,
                  neigh_area_latitude float,
                  neigh_area_longitude float,
                  neigh_transit_stops integer,
                  neigh_colleges_univs integer,
                  neigh_cultural_attractions integer,
                  neigh_places_of_interest integer,
                  neigh_pop_2016 float,
                  neigh_youth_15_24 float,
                  neigh_work_age_25_54 float
              )
              """

    for create_table_sql, table_name in zip(
        [query_1, query_2], ["trips", "station_stats"]
    ):
        _ = cur.execute(create_table_sql)
        if use_prefect:
            logger.info(f"Created table {table_name}")
        else:
            print(f"Created table {table_name}")
    cur.close()
    conn.close()


@task
def add_data_to_trips_table(
    trips_table_name: str,
    trips_stage_name: str,
    db_name: str,
    use_prefect: bool = False,
) -> None:
    """Add staged gzip-compressed CSV files to trips table."""
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    query = f"""
            COPY INTO {trips_table_name} from @{trips_stage_name}
            """
    _ = cur.execute(query)
    log_str = (
        f"Added files from stage {trips_stage_name} to table "
        f"{trips_table_name}"
    )
    if use_prefect:
        logger = prefect.context.get("logger")
        logger.info(log_str)
    else:
        print(log_str)
    cur.close()
    conn.close()


@task
def add_dataframe_to_stations_table(
    df: pd.DataFrame,
    stations_table_name: str,
    db_name: str,
    use_prefect: bool = False,
) -> None:
    """Write DataFrame to bikeshare stations table."""
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    success, _, nrows, _ = write_pandas(
        conn,
        df.drop(columns=["GEOMETRY"]),
        stations_table_name.upper(),
    )
    try:
        assert success
        assert nrows == len(df)
        log_str = (
            f"Exported: {len(df):,} rows to "
            f"{stations_table_name}, as expected"
        )
    except AssertionError:
        log_str = f"Expected: {len(df):,} rows\nActual: {nrows:,} rows"
    if use_prefect:
        logger = prefect.context.get("logger")
        logger.info(log_str)
    else:
        print(log_str)
    cur.close()
    conn.close()


@task
def delete_snowflake_resources(
    trips_table_name: str,
    station_stats_table_name: str,
    trips_stage_name: str,
    trips_file_format_name: str,
    db_name: str,
    stations_db_name: str,
    use_prefect: bool = False,
) -> None:
    """Delete Snowflake trips resources."""
    snowflake_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    snowflake_station_stats_dict = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=stations_db_name,
        schema=os.getenv("SNOWFLAKE_DB_SCHEMA"),
        role="sysadmin",
    )
    snowflake_dict_no_db = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role="sysadmin",
    )
    conn = snowflake.connector.connect(**snowflake_dict)
    cur = conn.cursor()
    queries = [
        f"""
        DROP STAGE {trips_stage_name}
        """,
        f"""
        DROP FILE FORMAT {trips_file_format_name}
        """,
        f"""
        DROP TABLE {trips_table_name}
        """,
    ]
    for query, so in zip(
        queries, ["stage", "file-format", "trips-table", "stations-table"]
    ):
        _ = cur.execute(query)
        log_str = f"Dropped {so}"
        if use_prefect:
            logger = prefect.context.get("logger")
            logger.info(log_str)
        else:
            print(log_str)
    cur.close()
    conn.close()

    conn = snowflake.connector.connect(**snowflake_station_stats_dict)
    cur = conn.cursor()
    query = f"""
            DROP TABLE {station_stats_table_name}
            """
    _ = cur.execute(query)
    cur.close()
    conn.close()

    conn = snowflake.connector.connect(**snowflake_dict_no_db)
    cur = conn.cursor()
    for database_name in [db_name, stations_db_name]:
        query = f"""
                DROP DATABASE {database_name}
                """
        _ = cur.execute(query)
    cur.close()
    conn.close()


@task
def delete_local_data_files(
    data_dir: str,
    use_prefect: bool = False,
) -> None:
    """Delete local trips and supplementary data files."""
    for f in glob(f"{data_dir}/raw/*.csv"):
        os.remove(f)

    raw_data_dirs = glob(f"{data_dir}/raw/*")
    for pdir in raw_data_dirs:
        if os.path.isdir(pdir):
            shutil.rmtree(pdir)

    for f in glob(f"{data_dir}/processed/*.csv.gz"):
        os.remove(f)
    log_str = f"Deleted local files in {data_dir} directory"
    if use_prefect:
        logger = prefect.context.get("logger")
        logger.info(log_str)
    else:
        print(log_str)
