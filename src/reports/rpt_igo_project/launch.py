import os
import inquirer


def main():
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        SPATIALITE_PATH - path to the mod_spatialite library (REQUIRED)
        DATA_ROOT - Path to the outputs folder. A subfolder rpt-igo-project will be created if it does not exist (REQUIRED)

        IGO_AOI_GEOJSON - path to the input geojson file for rpt-igo-project (optional)
        IGO_REPORT_NAME - name for the report (optional)
        IGO_PARQUET_PATH - path to an existing Athena UNLOAD Parquet folder/file (optional)
        IGO_KEEP_PARQUET - set to '1' or 'true' to retain downloaded Parquet files (optional)

    """
    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n")
    data_root = os.environ.get("DATA_ROOT")

    if not os.environ.get("SPATIALITE_PATH"):
        raise RuntimeError("\nSPATIALITE_PATH environment variable is not set. Please set it in your .env file\n\n  e.g. (on Mac) SPATIALITE_PATH=/opt/homebrew/lib/mod_spatialite.8.dylib \n (on PC) SPATIALITE_PATH=C:\\OSGeo4W\\bin\\mod_spatialite.dll")
    spatialite_path = os.environ.get("SPATIALITE_PATH")

    # IF we have everything we need from environment variables then we can skip the prompts
    if os.environ.get("IGO_AOI_GEOJSON"):
        if not os.path.exists(os.environ.get("IGO_AOI_GEOJSON")):
            raise RuntimeError(f"\nThe IGO_AOI_GEOJSON environment variable is set to '{os.environ.get('IGO_AOI_GEOJSON')}' but that file does not exist. Please fix or unset the variable to choose manually.\n")
        geojson_file = os.environ.get("IGO_AOI_GEOJSON")
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the example folder
        base_dir = os.path.dirname(__file__)

        # Use inquirer to choose a geojson file in the  "{env:DATA_ROOT}/rpt-igo-project/example" directory
        geojson_question = inquirer.prompt([
            inquirer.List(
                'geojson',
                message="Select a geojson file to use as the AOI",
                choices=[
                    f for f in os.listdir(os.path.join(base_dir, "example")) if f.endswith('.geojson')
                ],
            ),
        ])
        if geojson_question is None:
            print("\nNo geojson file selected. Exiting.\n")
            exit(0)
        geojson_filename = geojson_question['geojson']
        geojson_file = os.path.abspath(os.path.join(base_dir, "example", geojson_filename))

    if os.environ.get("IGO_REPORT_NAME"):
        report_name = os.environ.get("IGO_REPORT_NAME")
    else:
        report_name = geojson_file.split(os.path.sep)[-1].replace('.geojson', '').replace(' ', '_') + " - IGO Scrape"

    parquet_path = os.environ.get("IGO_PARQUET_PATH")
    if parquet_path and not os.path.exists(parquet_path):
        raise RuntimeError(
            f"\nIGO_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable.\n"
        )

    else:
        parquet_prompt = inquirer.prompt([
            inquirer.Text(
                'parquet_path',
                message='Optional: path to the Parquet folder or file to use for results (leave blank to query Athena)',
                default="",
            )
        ])
        if parquet_prompt:
            parquet_path = parquet_prompt.get('parquet_path')
            parquet_path = parquet_path.strip().strip('"').strip("'")

    # The final argument array we pass back
    args = [
        spatialite_path,
        os.path.join(data_root, "rpt-igo-project", report_name.replace(" ", "_")),
        geojson_file,
        report_name,
    ]

    if parquet_path:
        args.append("--use-parquet")
        args.append(parquet_path)

    keep_parquet_env = os.environ.get("IGO_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = keep_parquet_env.lower() in {"1", "true", "yes"}
    else:
        keep_answer = inquirer.prompt([
            inquirer.Confirm(
                'keep_parquet',
                message='Keep downloaded Parquet files after processing?',
                default=False,
            )
        ])
        keep_parquet = bool(keep_answer and keep_answer.get('keep_parquet'))

    if keep_parquet:
        args.append("--keep-parquet")

    return args
