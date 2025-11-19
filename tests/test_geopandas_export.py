from pathlib import Path
import geopandas as gpd
from shapely.geometry import Polygon

polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

project_dir_path = Path(r"C:\nardata\pydataroot\tmp")
aoi_path = project_dir_path / 'aoi.gpkg'
gdf.to_file(aoi_path)
