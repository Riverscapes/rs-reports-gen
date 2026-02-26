"""trying out the lonboard library for geospatial visualizatoin
a couple uv add statements and got a working map, when you click on it you get the feature properties
"""
from pathlib import Path
import geopandas as gpd
from lonboard import viz
from fiona import listlayers

# Worked!
path_to_shape = Path(r'C:\nardata\localcode\rs-reports-gen\src\reports\rpt_riverscapes_inventory\example\las_cruces_office.geojson')
gdf = gpd.read_file(path_to_shape)
viz(gdf)

# Also Worked! Gave warning Lonboard is only able to render data in EPSG:4326 projection.
path_to_gpkg = Path(r"C:\nardata\datadownload\blm\pasture_polygons_2026-02-23\BLM_Natl_Grazing_Pasture_Polygons_1561982987941612989.gpkg")
print(listlayers(path_to_gpkg))
gdf = gpd.read_file(path_to_gpkg, layer='BLM_Natl_Grazing_Pasture_Polygons')
viz(gdf)
