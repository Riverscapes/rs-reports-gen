# Automation Output Artifacts Policy

## Purpose

Define which report artifacts should be uploaded as standalone files versus zip-only in Fargate automation scripts.

## Default Pattern (most reports)

Most report scripts currently upload all generated files individually and also upload report.zip.

Benefits:

- Direct HTTP access to individual artifacts.
- Easier debugging and ad hoc downloads from API-managed object storage.

Trade-off:

- Duplicate storage/transfer cost for files that appear both standalone and inside report.zip.

## IGO Scraper Exception

The IGO scraper produces a large Riverscapes project bundle, including a GeoPackage that can be large.
Uploading all files both standalone and inside report.zip creates unnecessary duplication.

For IGO automation, use this staged pattern:

1. Write full project outputs into a subfolder (for example output/project).
2. Create report.zip from that full project folder.
3. Promote selected user-facing artifacts to the top-level output folder: report.html, report.pdf (if generated), and report.log.
4. Delete the staged unzipped project folder.
5. Upload top-level outputs.

Result:

- Frontend and users still get direct access to key report artifacts.
- Large internals (especially GeoPackage contents) are zip-only.

## Standardization Direction

When standardizing scripts, keep this decision explicit per report type:

- If individual outputs are typically useful (for example CSV or parquet workflows), upload all files + zip.
- If outputs are very large or mostly only useful as a project package (for example GeoPackage-heavy bundles), keep heavy internals zip-only and expose only key top-level artifacts.
