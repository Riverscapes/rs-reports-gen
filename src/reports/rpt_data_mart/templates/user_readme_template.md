# Riverscapes Data Mart Export

**Version**: {{ report_version }}

## Overview

This **Data Mart** export provides an enriched representation of Riverscapes datasets, optimally structured for self-service analytics consumers using Power BI, Jupyter Notebooks, or other data science and GIS tools.

The export contains pre-processed, flattened representations of the geodata found in Riverscapes Projects, formatted as column-oriented Parquet files. Data is enriched with derived metrics, condition bins, units, and localized metadata.

## Contents

This package contains the following artifacts:

- **`exports/`**: Contains the output Parquet files.
  - `dgo.parquet`: Riverscapes drainage network attributes and metrics, binned into standardized categories.
  - `huc.parquet`: Watershed boundary statistics and metadata.
  - `vegetation_cover.parquet`: (Optional) Vegetation cover timeseries extracted via Climate Engine.
  - `attains.parquet`: (Optional) EPA water quality assessments.
- **`data_dictionary.csv`**: Contains comprehensive field-level metadata for all exported columns. This includes original and friendly names, definitions, data types, and explicit units applied.
- **`pbi/`**: (If generated) Contains a ready-to-use Power BI (.pbip) project templated from the data dictionary.
- **`data_mart.log`**: Build process execution logs, identifying any warnings, geometries simplified, or external dependencies (like ATTAINS) skipped.
- **`readme.md`**: the instructions you are reading now, in markdown format
- **`project.html`**: the instructions you are reading now, in html format

## How to use this data

## Connecting to the data remotely

You don't need to download the data to work with it. This is especially handy for large, public reports. For personal reports, keep in mind the files are removed from the server after 7 days.

Look for the report ID on the platform. It is a 36-character sequence of letters and numbers. The export root path can then be build from this, e.g. `https://reports.riverscapes.net/public/88cbee03-6ee5-4b26-8094-234abe9a6e28`. All the other files listed above are relative to this.

### Python / Jupyter

Parquet is heavily optimized for Python ecosystems via `pandas` and `geopandas`.

```python
import pandas as pd
# Load tabular data
df = pd.read_parquet("exports/dgo.parquet")

# If you generated the export with `--include-geometry`, you can load it as a geospatial dataframe
# import geopandas as gpd
# gdf = gpd.read_parquet("exports/dgo.parquet")
```

### Power BI

If you enabled `--generate-pbi`, you can open the `.pbip` project directory within `pbi/`. It has been generated dynamically with relationships and metadata corresponding to your specific extract.

#### Connecting to alternate data source locations

The supplied pbip file comes with a parameter, `DataMartRoot` that should be populated with the path to the root folder containing the `exports` folder of data. See above for getting the remote URL.

- For web data, set it to a URL root like `https://reports.riverscapes.net/public/<report_id>`.
- For local data, set it to a local folder path like `C:\Data\my_export`.

The generated model uses a custom Power Query function `fn_LoadParquet` that defaults to web loading with `Web.Contents` + `Binary.Buffer`.

If you want local loading, edit `fn_LoadParquet` once in `expressions.tmdl` by uncommenting the `File.Contents` line and commenting the `Web.Contents` line.

`DataMartRoot` should not include a trailing slash.

#### Merging data models

Using the [TMDL View in Power BI Desktop](https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-tmdl-view), you can quickly apply many changes to semantic models. For this model, you will need the:

- Tables
- Expressions
- Relationships

## Units & Metadata

A core value of the Riverscapes Data Mart is the normalization of data units and metadata. Review `data_dictionary.csv` to ensure you are familiar with the calculated data metric bins and dimensions.
