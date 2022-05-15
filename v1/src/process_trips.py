#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Process raw bikeshare trips data."""

# pylint: disable=invalid-name


import pandas as pd
import pandera as pa

datetime_attrs_dtypes = {
    f"{trip_point}_{date_attr}": pa.Column(pa.Int)
    for trip_point in ["START", "END"]
    for date_attr in ["year", "month", "day", "hour", "minute", "quarter"]
}
datetime_attrs_dtypes.update({"DURATION": pa.Column(pa.Int)})
trips_schema_processed = {
    "TRIP_ID": pa.Column(pa.Int),
    "TRIP__DURATION": pa.Column(pa.Int),
    "START_STATION_ID": pa.Column(pa.Int, nullable=True),
    "START_TIME": pa.Column(
        pa.Timestamp, checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))]
    ),
    "START_STATION_NAME": pa.Column(pd.StringDtype()),
    "END_STATION_ID": pa.Column(pa.Int, nullable=True),
    "END_TIME": pa.Column(
        pa.Timestamp, checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))]
    ),
    "END_STATION_NAME": pa.Column(pd.StringDtype()),
    "BIKE_ID": pa.Column(pa.Int, nullable=True),
    "USER_TYPE": pa.Column(
        pd.StringDtype(),
        checks=[
            pa.Check(
                lambda s: s.isin(["Annual Member", "Casual Member"]),
            )
        ],
    ),
}
trips_schema_processed.update(datetime_attrs_dtypes)
trips_schema_processed = pa.DataFrameSchema(
    columns=trips_schema_processed,
    index=pa.Index(pa.Int),
)

datetime_attrs_dtypes_v2 = {
    f"{trip_point}_{date_attr}": pa.Column(pa.Int)
    for trip_point in ["START"]
    for date_attr in ["year", "month", "hour"]
}
datetime_attrs_dtypes_v2.update({"START_weekday": pa.Column(pd.StringDtype())})
trips_schema_processed_v2 = {
    "TRIP_ID": pa.Column(pa.Int),
    "TRIP__DURATION": pa.Column(pa.Int),
    "START_STATION_ID": pa.Column(pa.Int, nullable=True),
    "START_TIME": pa.Column(
        pa.Timestamp, checks=[pa.Check(lambda s: s.dt.year.isin([2021, 2022]))]
    ),
    "START_STATION_NAME": pa.Column(pd.StringDtype()),
    "USER_TYPE": pa.Column(
        pd.StringDtype(),
        checks=[
            pa.Check(
                lambda s: s.isin(["Annual Member", "Casual Member"]),
            )
        ],
    ),
}
trips_schema_processed_v2.update(datetime_attrs_dtypes_v2)
trips_schema_processed_v2 = pa.DataFrameSchema(
    columns=trips_schema_processed_v2,
    index=pa.Index(pa.Int),
)


def add_datepart(df: pd.DataFrame) -> pd.DataFrame:
    """Add datetime attributes."""
    # Extract attributes
    # # Trip duration
    # df["DURATION"] = (df["END_TIME"] - df["START_TIME"]).dt.seconds
    # # Datetime attributes
    # for trip_point in ["START", "END"]:
    for trip_point in ["START"]:
        df[f"{trip_point}_year"] = df[f"{trip_point}_TIME"].dt.year
        df[f"{trip_point}_month"] = df[f"{trip_point}_TIME"].dt.month
        # df[f"{trip_point}_day"] = df[f"{trip_point}_TIME"].dt.day
        df[f"{trip_point}_weekday"] = df[f"{trip_point}_TIME"].dt.day_name()
        df[f"{trip_point}_hour"] = df[f"{trip_point}_TIME"].dt.hour
        # df[f"{trip_point}_minute"] = df[f"{trip_point}_TIME"].dt.minute
    df["START_weekday"] = df["START_weekday"].astype(pd.StringDtype())
    return df


@pa.check_output(trips_schema_processed_v2)
def process_trips_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process raw ridership data."""
    df = add_datepart(df)
    return df
