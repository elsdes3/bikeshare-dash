#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Plotly Dash Application Code."""

# pylint: disable=invalid-name,too-many-locals

import pandas as pd
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

import py_helpers as ph

hourly_trips_by_user = pd.read_csv(
    "data/processed/bikeshare_hourly_aggregations.csv"
)
daily_trips_by_user = pd.read_csv(
    "data/processed/bikeshare_daily_aggregations.csv", parse_dates=["date"]
)
daily_trips_by_user_w_weather = pd.read_csv(
    "data/processed/bikeshare_daily_aggregations_for_weather.csv",
    parse_dates=["date"],
)

neighbourhoods = [
    "Waterfront Communities-The Island (77)",
    "Niagara (82)",
    "Church-Yonge Corridor (75)",
    "North St.James Town (74)",
    "Regent Park (72)",
    "All",
]
months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "All",
]

external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
        "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]
app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
app.title = "Bikeshare Toronto Analytics: Understand Your Users!"

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.P(children="🚴", className="header-emoji"),
                html.H1(
                    children="Toronto Bikeshare 2021 Insights",
                    className="header-title",
                ),
                html.P(
                    children=(
                        "Analyze the behavior of bikeshare users\n"
                        "during the year 2021"
                    ),
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(
                            children="Neighbourhood", className="menu-title"
                        ),
                        dcc.Dropdown(
                            id="neighbourhood-filter",
                            options=[
                                {"label": neigh, "value": neigh}
                                for neigh in neighbourhoods
                            ],
                            value="All",
                            clearable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="Month", className="menu-title"),
                        dcc.Dropdown(
                            id="month-filter",
                            options=[
                                {"label": month, "value": month}
                                for month in months
                            ],
                            value="All",
                            clearable=False,
                            className="dropdown",
                        ),
                    ]
                ),
            ],
            className="menu",
        ),
        html.Div(
            [
                html.Div(
                    html.H2(
                        children="When are Members Using Toronto Bikeshare?",
                        className="section-header-title",
                    ),
                    className="section-header-card",
                )
            ],
            className="section-header-wrapper",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="daily-chart",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="weekday-chart",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="hourly-chart",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
        html.Div(
            [
                html.Div(
                    html.H2(
                        children="How does Weather Affect Ridership?",
                        className="section-header-title",
                    ),
                    className="section-header-card",
                )
            ],
            className="section-header-wrapper",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="daily-chart-weather",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    ]
)


@app.callback(
    [
        Output("hourly-chart", "figure"),
        Output("weekday-chart", "figure"),
        Output("daily-chart", "figure"),
        Output("daily-chart-weather", "figure"),
    ],
    [
        Input("neighbourhood-filter", "value"),
        Input("month-filter", "value"),
    ],
)
def update_charts(neighbourhood, month):
    """Update Charts Based on User Selection."""
    l_colors = ["black", "#BEBEBE"]
    m_types = ["Annual", "Casual"]
    neighbourhoods_list = [
        "Waterfront Communities-The Island (77)",
        "Niagara (82)",
        "Church-Yonge Corridor (75)",
        "North St.James Town (74)",
        "Regent Park (72)",
        "All",
    ]

    rank = neighbourhoods_list.index(neighbourhood) + 1
    neigh_suffix = (
        f" in {neighbourhood.split(' (')[0]}" if neighbourhood != "All" else ""
    )
    neigh_suffix_with_rank = (
        (
            f" in {neighbourhood.split(' (')[0]} (rank {rank} by "
            "avg. daily departures)"
        )
        if neighbourhood != "All"
        else ""
    )

    # Filter data based on user selections
    month_mask = f" & month == '{month}'" if month != "All" else ""
    neigh_mask = f"area_name == '{neighbourhood}'"
    data_filtered = daily_trips_by_user.query(f"{neigh_mask}{month_mask}")
    data_filter_for_static_facet_chart = daily_trips_by_user.query(neigh_mask)

    # Daily line chart
    daily_trips_by_user_by_neigh = ph.plot_daily_trips_line_chart(
        data_filtered,
        month=month,
        l_colors=l_colors,
        x="date",
        y="num_trips",
        m_types=m_types,
        annotation_texts=[
            "Has a<br>Peak Season",
            "Shows higher<br>weekly variance",
        ],
        ptitle=f"Daily Trips{neigh_suffix_with_rank} in ",
    )

    # Daily faceted bar chart
    quarter = 3
    m_names_dict = {
        1: ["January", "February", "March"],
        2: ["April", "May", "June"],
        3: ["July", "August", "September"],
    }
    d_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    months_order = m_names_dict[quarter]
    # # Transform data
    quarter_weekday_trips_by_user = (
        data_filter_for_static_facet_chart.assign(
            weekday=data_filter_for_static_facet_chart["date"].dt.day_name()
        )
        .assign(quarter=data_filter_for_static_facet_chart["date"].dt.quarter)
        .query(f"quarter == {quarter}")
        .groupby(
            ["user_type", "quarter", "month", "weekday"],
            as_index=False,
        )["num_trips"]
        .mean()
        .set_index("weekday")
        .loc[d_names]
        .reset_index()
        .set_index("month")
        .loc[months_order]
        .reset_index()
    )
    quarter_weekday_trips_by_user["weekday"] = quarter_weekday_trips_by_user[
        "weekday"
    ].str[:3]
    # # Title
    ptitle_suffix = f" in Q{quarter} of 2021" + neigh_suffix
    # # Plot
    weekday_trips_by_q_fig = ph.plot_faceted_bar_chart(
        quarter_weekday_trips_by_user,
        x="weekday",
        y="num_trips",
        color_by_col="user_type",
        facet_col="month",
        colors=["black", "#BEBEBE"],
        ptitle=f"Monthly Trips by Weekday{ptitle_suffix}",
        facet_col_vals=m_names_dict[quarter],
    )

    # Hourly
    hourly_trips_by_user_by_neigh = hourly_trips_by_user.query(neigh_mask)
    hourly_grouped_barchart_fig = ph.plot_grouped_bar_chart(
        hourly_trips_by_user_by_neigh,
        l_colors,
        m_types,
        f"Hourly Trips{neigh_suffix} in 2021",
    )
    ptitle = f"Relationship between Ridership and Temp.{neigh_suffix} in 2021"

    # Daily scatter with weather
    daily_trips_weather_filtered = daily_trips_by_user_w_weather.query(
        f"area_name == '{neighbourhood}'"
    )
    daily_trips_by_user_by_neigh_weather_fig = ph.plot_scatter(
        daily_trips_weather_filtered,
        x="tavg",
        y="num_trips",
        m_types=["Annual", "Casual"],
        l_colors=["black", "#BEBEBE"],
        ptitle=ptitle,
    )

    return [
        hourly_grouped_barchart_fig,
        weekday_trips_by_q_fig,
        daily_trips_by_user_by_neigh,
        daily_trips_by_user_by_neigh_weather_fig,
    ]


if __name__ == "__main__":
    app.run_server(debug=True)
