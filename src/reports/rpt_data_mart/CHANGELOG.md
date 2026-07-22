# Version History

## 0.2.0 2026-July

* adding additional intersection layers (starting with updated BLM Pastures for Elko-Vale)

## 0.1.7 2026-April

* bug fixes in unit handling
* NOTE 'pasture_id' and 'HUC10_Code' in the NID layer were manually added and populated for BLM - they are not yet part of the automated generation

## 0.1.6 2026-April

* unnest dem_bins from within the rscontext_huc10 parquet -- allows for proper field definition and unit conversion

## 0.1.5 2026-April

* adding ~80 additional DGO columns
* adding updated New Mexico Bootheel pastures layer, spatially intersected with DGOs
* adding National Inventory of Dams extract (calls USACE API)
* making larger units default for most metrics (e.g. miles vs ft, acres vs ft²)

## 0.1.4 2026-April

* improve Power BI data loading (web or local)

## 0.1.3 2026-April

* remove grazing allotments table
* connects to new Athena objects - materialized for entire nation, not just New Mexico
* performance improvements in HUC10 query
* adds join between dgos & huc10

## 0.1.2 March 2026

* rename table `huc` to `huc10_rscontext`
* add pastures table
* add relationships between tables to Power BI model

## 0.1.1 March 2026

Initial version - Proof of Concept. Includes:

* Queries/clips and exports as Parquet:  RME (dgo), rscontext (HUC10), Grazing Allotments, Climate Engine and EPA Attains data
* generated `.pbip` Power BI output (using enhanced report format .PBIR)
* report.html

### Note

* assumes intersection with dgos is precomputed
* true for New Mexico only
