# About this project

This project generates a Riverscapes Project of type `igos` by scraping raw_rme for a supplied area of interest. It's not really a 'report' _per se_.

The output GeoPackage now includes an additional `huc10` polygon layer generated from `default.wbdhu10_cleaned` for the AOI, including joined latest-source-project attributes and per-HUC10 source-project counts.
