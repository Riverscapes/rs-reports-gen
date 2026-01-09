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
            RNS_AOI_GEOJSON - path to the input geojson file for rpt-rivers-need-space (optional)
            RNS_REPORT_NAME - name for the report (optional)
            RNS_CSV - optional path to a CSV file to use instead of querying Athena (optional)

    """

    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))
    data_root = os.environ.get("DATA_ROOT")

    # IF we have everything we need from environment variables then we can skip the prompts
    if os.environ.get("RNS_AOI_GEOJSON"):
        if not os.path.exists(os.path.join(os.environ.get("RNS_AOI_GEOJSON"))):
            raise RuntimeError(
                colored(f"\nThe RNS_AOI_GEOJSON environment variable is set to '{os.environ.get('RNS_AOI_GEOJSON')}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
        geojson_file = Path(os.environ.get("RNS_AOI_GEOJSON"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the example folder
        base_dir = os.path.dirname(__file__)

        # Use inquirer to choose a geojson file in the  "{env:DATA_ROOT}/rpt-rivers-need-space/example" directory
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
        geojson_file = Path(os.path.abspath(os.path.join(base_dir, "example", geojson_filename)))

    # Now ask for an optional csv path
    csv_question = inquirer.prompt([
        inquirer.Text(
            'csv',
            message="Optional: Enter a path to a CSV file to use for results (leave blank to query Athena)",
            default="",
        ),
    ])
    csv_file = csv_question['csv']
    # Strip leading/trailing quotes if present
    if csv_file:
        csv_file = csv_file.strip().strip('"').strip("'")

    if os.environ.get("UNIT_SYSTEM"):
        unit_system = os.environ.get("UNIT_SYSTEM")
        if unit_system not in ["SI", "imperial"]:
            raise RuntimeError(colored(f"\nThe UNIT_SYSTEM environment variable is set to '{unit_system}' but it must be either 'SI' or 'imperial'. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # Ask for unit system
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
        unit_system = unit_system_question['unit_system']

    if os.environ.get("RNS_REPORT_NAME"):
        report_name = os.environ.get("RNS_REPORT_NAME")
    else:
        report_name = geojson_file.stem.replace(' ', '_') + " - Rivers Need Space"

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
        include_pdf = include_pdf_question['include_pdf']

    args = [
        os.path.join(data_root, "rpt-rivers-need-space", report_name.replace(" ", "_")),
        geojson_file,
        report_name,
        "--unit_system", unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")
    if csv_file.strip():
        args.append("--csv")
        args.append(csv_file.strip())

    return args
