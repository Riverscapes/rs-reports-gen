# Version History

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
