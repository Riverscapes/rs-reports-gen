import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go

from util.figures import make_point_map_with_aoi, point_df_to_gdf


def build_pbr_figures(main_data: pd.DataFrame, aoi: gpd.GeoDataFrame) -> dict[str, go.Figure]:
    """returns: dictionary of figures for PBR report"""
    figures = {
        "map": make_point_map_with_aoi(
            point_df_to_gdf(main_data, 'location.latitude', 'location.longitude'),
            aoi,
            hover_cols=["name", "watershedName", "date_IMPLEMENTEDxxxx"],
            point_name="PBR Projects",
            point_size=10,
        )
    }
    return figures
