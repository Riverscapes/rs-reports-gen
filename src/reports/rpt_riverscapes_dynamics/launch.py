import os
from pathlib import Path
import inquirer
from termcolor import colored


def main():
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        For all reports:
            DATA_ROOT - Path to the outputs folder. A subfolder rpt-rivers-need-space will be created if it does not exist (REQUIRED)
            UNIT_SYSTEM - unit system to use: "SI" or "imperial" (optional, default is "SI")
            INCLUDE_PDF - whether to include a PDF version of the report (optional, default is True)

        Report-specific variables:
            RDYN_AOI_GEOJSON - path to the input geojson file for rpt-rivers-need-space (optional)
            RDYN_REPORT_NAME - name for the report (optional)
            RDYN_PARQUET_PATH - path to an existing Athena UNLOAD Parquet folder/file (optional)
            RDYN_KEEP_PARQUET - set to '1' or 'true' to retain downloaded Parquet files (optional)

    """

    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))
    data_root = os.environ.get("DATA_ROOT")

    # IF we have everything we need from environment variables then we can skip the prompts
    rsi_aoi_geojson = os.environ.get("RDYN_AOI_GEOJSON")
    if rsi_aoi_geojson:
        geojson_file = Path(rsi_aoi_geojson)
        if not geojson_file.exists():
            raise RuntimeError(
                colored(f"\nThe RDYN_AOI_GEOJSON environment variable is set to '{os.environ.get('RDYN_AOI_GEOJSON')}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the code example folder
        base_dir = Path(__file__).parent
        geojson_question = inquirer.prompt([
            inquirer.List(
                'geojson',
                message="Select a geojson file to use as the AOI",
                choices=[
                    f for f in os.listdir(os.path.join(base_dir, "example")) if f.endswith('.geojson')
                ],
            ),
        ])
        if not geojson_question or 'geojson' not in geojson_question:
            print("\nNo geojson file selected. Exiting.\n")
            return
        geojson_filename = geojson_question['geojson']
        geojson_file = Path(base_dir / "example" / geojson_filename).absolute()

    if os.environ.get("UNIT_SYSTEM"):
        unit_system = os.environ.get("UNIT_SYSTEM")
        if unit_system not in ["SI", "imperial"]:
            raise RuntimeError(colored(f"\nThe UNIT_SYSTEM environment variable is set to '{unit_system}' but it must be either 'SI' or 'imperial'. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        unit_system_question = inquirer.prompt([
            inquirer.List(
                'unit_system',
                message="Select a unit system to use",
                choices=[
                    "SI",
                    "imperial"
                ],
                default="SI"
            ),
        ])
        if not unit_system_question or 'unit_system' not in unit_system_question:
            print("\nNo unit system selected. Exiting.\n")
            return
        unit_system = unit_system_question['unit_system']

    if os.environ.get("RDYN_REPORT_NAME"):
        report_name = os.environ.get("RDYN_REPORT_NAME")
    else:
        report_name = geojson_file.stem.replace(' ', '_') + " - Riverscapes Dynamics"

    # Ask for whether or not to include PDF. Default to NO
    if os.environ.get("INCLUDE_PDF"):
        include_pdf = os.environ.get("INCLUDE_PDF", None) is not None
    else:
        include_pdf_question = inquirer.prompt([
            inquirer.Confirm(
                'include_pdf',
                message="Include a PDF version of the report? (Default is No)",
                default=False
            ),
        ])
        if not include_pdf_question or 'include_pdf' not in include_pdf_question:
            print("\nNo PDF option selected. Exiting.\n")
            return None
        include_pdf = include_pdf_question['include_pdf']

    parquet_path = os.environ.get("RDYN_PARQUET_PATH")
    if parquet_path and not os.path.exists(parquet_path):
        raise RuntimeError(
            f"\nRDYN_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable.\n"
        )

    else:
        parquet_prompt = inquirer.prompt([
            inquirer.Text(
                'parquet_path',
                message='Optional: path to the Parquet folder or file to use for results (leave blank to query Athena)',
                default="",
            )
        ])
        if not parquet_prompt or 'parquet_path' not in parquet_prompt:
            print("\nNo Parquet path selected. Exiting.\n")
            return
        parquet_path = parquet_prompt.get('parquet_path')
        parquet_path = parquet_path.strip().strip('"').strip("'")

    args = [
        os.path.join(data_root, "rpt-riverscapes-dynamics", report_name.replace(" ", "_")),
        geojson_file,
        report_name,
        "--unit_system", unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")

    if parquet_path:
        args.append("--use-parquet")
        args.append(parquet_path)

    keep_parquet_env = os.environ.get("RDYN_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = keep_parquet_env.lower() in {"1", "true", "yes"}
    else:
        # if we were supplied Parquet let's assume we want to keep it
        if parquet_path:
            keep_parquet = True
        else:
            keep_answer = inquirer.prompt([
                inquirer.Confirm(
                    'keep_parquet',
                    message='Keep downloaded Parquet files after processing?',
                    default=False,
                )
            ])
            if not keep_answer or 'keep_parquet' not in keep_answer:
                print("\nNo keep_parquet option selected. Exiting.\n")
                return
            keep_parquet = bool(keep_answer.get('keep_parquet'))

    if keep_parquet:
        args.append("--keep-parquet")

    return args
