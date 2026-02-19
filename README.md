# Riverscapes Report Generators Repository

This repository holds the code to generate reports based on Riverscapes Consortium data, largely maintained in AWS Athena.

It consists of a python project for each report generator, as well as scripts necessary to run the project with Fargate.

Code to maintain the web UI that allows users to trigger report generation with custom parameters is in another repository, [`rs-reports-monorepo`](https://github.com/Riverscapes/rs-reports-monorepo).

## Setting up & running your own instance

Use `uv sync`. If you're going to make any changes, there are additional libraries used for development. Run `uv sync --extra dev` instead.

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

## Jupyter Notebook Output Stripping (nbstripout)

To keep notebook outputs (plots, binary, HTML, etc.) out of git commits, we use [nbstripout](https://github.com/kynan/nbstripout?tab=readme-ov-file) via pre-commit hooks. 

### Setup (one-time per clone)

1. Install all dev dependencies:
2. Install the pre-commit hook:

```sh
uv sync --extra dev
uv run pre-commit install
```

This ensures that any .ipynb files you commit will have their output cells automatically stripped.

### Usage

- Just commit as usual. The hook will run automatically.
- If it changed your file you'll see this as an additional change to add to your commit
- If you want to commit the output, it is possible to set the `keep_output` **tag** on a cell or the notebook. See [nbstripout readme](https://github.com/kynan/nbstripout?tab=readme-ov-file#keeping-output-on-specific-cells).
- if you want the output for yourself, you might keep a copy in a folder named `localonly` which has been added to .gitignore
- To manually check use or run all hooks on all files:
  
```sh
uv run pre-commit run --all-files
```
