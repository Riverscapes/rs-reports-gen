"""Figure builders for the Inventory of Resources report.

Created 2026-08-13.
Created by copilot.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def length_bar_chart(summary_df: pd.DataFrame, category_column: str, title: str) -> go.Figure:
    """Build a horizontal stream-length chart from a summary table.

    Args:
            summary_df: Summary dataframe containing stream length by category.
            category_column: Category column to display on the y axis.
            title: Figure title.

    Returns:
            A Plotly bar chart.

    Created by copilot.
    """
    display_data = summary_df.head(15).sort_values("stream_length", ascending=True)
    figure = px.bar(
        display_data,
        x="stream_length",
        y=category_column,
        orientation="h",
        labels={"stream_length": "Total Stream Length (m)", category_column: category_column.replace("_", " ").title()},
        title=title,
    )
    figure.update_layout(margin={"r": 20, "t": 50, "l": 20, "b": 20}, showlegend=False)
    return figure


def distribution_chart(data_df: pd.DataFrame, value_column: str, title: str, x_label: str) -> go.Figure:
    """Build a histogram for a numeric inventory attribute.

    Args:
            data_df: Normalized inventory dataframe.
            value_column: Numeric source field.
            title: Figure title.
            x_label: Human-readable axis label.

    Returns:
            A Plotly histogram, including an empty-state annotation when needed.

    Created by copilot.
    """
    values = data_df.get(value_column, pd.Series(dtype=float)).dropna()
    figure = px.histogram(values, x=value_column, nbins=30, labels={value_column: x_label, "count": "Inventory Records"}, title=title)
    if values.empty:
        figure.add_annotation(text="No values reported for this area of interest.", showarrow=False)
    figure.update_layout(margin={"r": 20, "t": 50, "l": 20, "b": 20}, showlegend=False)
    return figure
