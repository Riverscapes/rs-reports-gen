import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go

from util.figures import make_aoi_outline_map


def build_pbr_figures(main_data: pd.DataFrame, aoi: gpd.GeoDataFrame) -> dict[str, go.Figure]:
    """returns: dictionary of figures for PBR report"""
    figures = {"map": make_aoi_outline_map(aoi)}
    return figures
