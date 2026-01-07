# Riverscapes Inventory Report

This report generates an inventory of riverscapes data for a given Area of Interest (AOI).

## Usage

```bash
python -m reports.rpt_riverscapes_inventory.main <output_path> <path_to_shape> <report_name> [options]
```

### Arguments

*   `output_path`: Folder to store the outputs (will be created).
*   `path_to_shape`: Path to the GeoJSON/Shapefile defining the AOI.
*   `report_name`: Name for the report.

### Options

*   `--include_pdf`: Generate a PDF version of the report.
*   `--unit_system`: Unit system to use: `SI` (default) or `imperial`.
*   `--no-nid`: **(New)** Disable fetching data from the USACE National Inventory of Dams (NID). By default, the report queries NID for dams within the AOI.

## Data Sources

*   **Riverscapes Context**: Fetched from AWS Athena.
*   **National Inventory of Dams (NID)**: Fetched from USACE geospatial API.
