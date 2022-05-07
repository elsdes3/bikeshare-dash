#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Plotting Charts with Plotly."""

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy.optimize import curve_fit

# pylint: disable=invalid-name,too-many-locals,too-many-arguments


def plot_grouped_bar_chart(
    df,
    l_colors,
    m_types,
    ptitle,
):
    """Create a grouped bar chart with Plotly."""
    layout = go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=go.layout.Margin(l=0, r=0, b=25, t=0),
    )
    fig = go.Figure(layout=layout)

    for m_type, l_color in zip(m_types, l_colors):
        df_d = df.query(f"user_type == '{m_type} Member'")
        fig.add_trace(
            go.Bar(
                name=m_type,
                x=df_d["hour"].astype(str),
                y=df_d["num_trips"],
                marker_color=l_color,
            ),
        )

    annotations = []
    # Title
    annotations.append(
        dict(
            xref="paper",
            yref="paper",
            x=0.05,
            y=0.95,
            xanchor="left",
            yanchor="bottom",
            text=ptitle,
            font=dict(family="Arial", size=22, color="rgb(92,90,90)"),
            showarrow=False,
        )
    )
    fig.update_xaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    fig.update_yaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    fig.update_layout(
        barmode="group",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.9925),
        annotations=annotations,
    )
    fig.update_layout(
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            # showticklabels=False,
        ),
        autosize=True,
        margin=dict(autoexpand=False, l=50, r=75, t=15, b=40),
        showlegend=True,
    )
    fig.update_layout(
        legend=dict(
            yanchor="top",
            y=0.9,
            xanchor="left",
            x=0.01,
            font=dict(family="Arial", size=16, color="rgb(92,90,90)"),
        )
    )
    return fig


def plot_daily_trips_line_chart(
    df,
    month,
    l_colors,
    x,
    y,
    m_types,
    annotation_texts,
    ptitle,
):
    """Show daily trips by user type."""
    layout = go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=go.layout.Margin(l=0, r=0, b=25, t=0),
    )
    fig = go.Figure(layout=layout)

    annotations = []
    for m_type, l_color, annotation_text in zip(
        m_types, l_colors, annotation_texts
    ):
        # Get final mask
        user_type_mask = f"user_type == '{m_type} Member'"
        # Filter data to Separate Users
        df_d = df.query(user_type_mask)[[x, y]]

        # Get suffix in title
        month_suffix = f"{month[:3]}. 2021" if month != "All" else "2021"

        # Lines
        fig.add_trace(
            go.Scatter(
                x=df_d[x],
                y=df_d[y],
                mode="lines",
                name=m_type,
                line=dict(color=l_color, width=2),
                connectgaps=True,
            )
        )

        # endpoints
        fig.add_trace(
            go.Scatter(
                x=[df_d[x].iloc[0], df_d[x].iloc[-1]],
                y=[df_d[y].iloc[0], df_d[y].iloc[-1]],
                mode="markers",
                marker=dict(color=l_color, size=8),
            )
        )

        # labeling the left_side of the plot
        annotations.append(
            dict(
                xref="paper",
                x=0.05,
                y=df_d[y].iloc[0],
                xanchor="right",
                yanchor="middle",
                text=m_type,
                font=dict(family="Arial", size=16, color=l_color),
                showarrow=False,
            )
        )

        # labeling the right_side of the plot
        annotation_text_updated = (
            annotation_text if month == "All" else f"{df_d[y].iloc[-1]:,}"
        )
        annotations.append(
            dict(
                xref="paper",
                x=0.95,
                y=df_d[y].iloc[-1],
                xanchor="left",
                yanchor="middle",
                text=annotation_text_updated,
                font=dict(family="Arial", size=16, color=l_color),
                showarrow=False,
            )
        )
    fig.update_layout(
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=False,
        ),
        xaxis=dict(showgrid=False),
        autosize=True,
        margin=dict(
            autoexpand=False,
            l=20,
            r=65,
        ),
        showlegend=False,
    )
    # Title
    annotations.append(
        dict(
            xref="paper",
            yref="paper",
            x=0.05,
            y=0.9,
            xanchor="left",
            yanchor="bottom",
            text=f"{ptitle}{month_suffix}",
            font=dict(family="Arial", size=22, color="rgb(92,90,90)"),
            showarrow=False,
        )
    )
    fig.update_layout(annotations=annotations)
    fig.update_xaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    for trace in fig["data"]:
        if trace["name"] not in ["Annual", "Casual"]:
            trace["showlegend"] = False
    return fig


def plot_faceted_bar_chart(
    df,
    x="weekday",
    y="num_trips",
    color_by_col="user_type",
    facet_col="month",
    colors=["black", "#BEBEBE"],
    ptitle="Title",
    facet_col_vals=[],
):
    """Create a faceted bar chart with Plotly."""
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color_by_col,
        barmode="group",
        facet_col=facet_col,
        color_discrete_sequence=colors,
    )
    fig.for_each_annotation(lambda a: a.update(text=""))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=False,
        ),
        autosize=True,
        margin=dict(autoexpand=False, l=20, r=45, t=30, b=40),
        showlegend=True,
    )
    fig.update_yaxes(title="")

    for k, v in enumerate(facet_col_vals, 1):
        fig.update_layout(
            {
                f"xaxis{k}": dict(
                    title=dict(
                        text=f"<b>{v}</b>",
                        font=dict(
                            family="Arial", size=16, color="rgb(92,90,90)"
                        ),
                    )
                )
            }
        )
    # Title
    annotation = dict(
        xref="paper",
        yref="paper",
        x=0.0,
        y=0.99,
        xanchor="left",
        yanchor="bottom",
        text=ptitle,
        font=dict(family="Arial", size=22, color="rgb(92,90,90)"),
        showarrow=False,
    )
    fig.add_annotation(annotation)
    fig.update_xaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    fig.update_layout(
        legend=dict(
            orientation="h",
            x=1,
            y=0.99,
            xanchor="right",
            yanchor="bottom",
            font=dict(family="Arial", size=16, color="rgb(92,90,90)"),
        ),
        legend_title=dict(text=""),
    )
    return fig


def exp_growth_no_shift(x, a, b):
    """Fit a curve to x-y data."""
    return a * np.exp(-b * x)


def plot_scatter(
    df,
    x="tavg",
    y="num_trips",
    m_types=["Annual", "Casual"],
    l_colors=["black", "#BEBEBE"],
    ptitle="Title",
):
    """Create a grouped scatter plot."""
    layout = go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=go.layout.Margin(l=0, r=0, b=25, t=0),
    )
    fig = go.Figure(layout=layout)

    for m_type, l_color in zip(m_types, l_colors):
        xvar = df.query(f"user_type == '{m_type} Member'")[x]
        yvar = df.query(f"user_type == '{m_type} Member'")[y]
        popt, _ = curve_fit(exp_growth_no_shift, xvar, yvar, maxfev=2000)
        y_fit = exp_growth_no_shift(xvar, *popt)

        # Scatter
        fig.add_trace(
            go.Scatter(
                x=xvar,
                y=yvar,
                mode="markers",
                name=m_type,
                marker=dict(color=l_color),
            )
        )
        # Trend Curves
        fig.add_trace(
            go.Scatter(
                x=xvar,
                y=y_fit,
                mode="lines",
                name=None,
                marker=dict(color=l_color),
            )
        )

    annotations = []
    # Title
    annotations.append(
        dict(
            xref="paper",
            yref="paper",
            x=0.05,
            y=0.95,
            xanchor="left",
            yanchor="bottom",
            text=ptitle,
            font=dict(family="Arial", size=22, color="rgb(92,90,90)"),
            showarrow=False,
        )
    )

    fig.update_xaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    fig.update_yaxes(
        tickfont=dict(family="Arial", size=16, color="rgb(92,90,90)")
    )
    fig.update_layout(
        legend=dict(
            yanchor="top",
            y=0.9,
            xanchor="left",
            x=0.05,
            font=dict(family="Arial", size=16, color="rgb(92,90,90)"),
        )
    )

    # set showlegend property by name of trace
    for trace in fig["data"]:
        if trace["name"] not in [m_type for m_type in m_types]:
            trace["showlegend"] = False

    fig.update_layout(
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=False,
        ),
        autosize=True,
        margin=dict(
            # autoexpand=False,
            l=0,
            r=0,
            t=15,
            b=20,
            pad=0,
        ),
        showlegend=True,
        annotations=annotations,
    )
    return fig
