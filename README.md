# Riverscapes Report Generators Repository

This repository holds the code to generate reports based on Riverscapes Consortium data, largely maintained in AWS Athena.

It consists of a python project for each report generator, as well as scripts necessary to run the project with Fargate.

Code to maintain the web UI that allows users to trigger report generation with custom parameters is in another repository, [`rs-reports-monorepo`](https://github.com/Riverscapes/rs-reports-monorepo).

## Setting up & running your own instance

Use `uv sync`. If you want additional libraries used for development, run `uv sync --extra dev` instead.

You may need to [install WeasyPrint following these instructions](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation).


## Running the "📋 Report Launcher" Task

If you run the "📋 Report Launcher" task from the launch.json file you will be prompted for which report to run and a series of choices to choose the parameters for that report. You can bypass these choices by setting optional environment variables in your `.env` file.

### Example .env file

```conf
# MANDATORY. You need to set these if you want the reports to run
DATA_ROOT=/where/i/store/my/data/rs_reports
SPATIALITE_PATH=/opt/homebrew/lib/mod_spatialite.8.dylib

# OPTIONAL OVERRIES FOR "Rivers Need Space" REPORT
# RNS_AOI_GEOJSON=/my/awesome/aoi.geojson
# RNS_REPORT_NAME='Steve the report'
# RNS_CSV=

# OPTIONAL OVERRIES FOR "IGO Project" REPORT
IGO_AOI_GEOJSON=/my/awesome/aoi.geojson 
IGO_REPORT_NAME='Gary'
```