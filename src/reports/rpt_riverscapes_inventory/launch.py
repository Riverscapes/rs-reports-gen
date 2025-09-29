import os
import inquirer
from termcolor import colored


def main():
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

        DATA_ROOT - Path to the outputs folder. A subfolder rpt-rivers-need-space will be created if it does not exist (REQUIRED)

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
        geojson_file = os.environ.get("RNS_AOI_GEOJSON")
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
        geojson_filename = geojson_question['geojson']
        geojson_file = os.path.abspath(os.path.join(base_dir, "example", geojson_filename))

    # Now ask for an optional csv path
    csv_question = inquirer.prompt([
        inquirer.Text(
            'csv',
            message="Optional: Enter a path to a CSV file to use for results (leave blank to query Athena)",
            default="",
        ),
    ])
    csv_file = csv_question['csv']

    if os.environ.get("RNS_REPORT_NAME"):
        report_name = os.environ.get("RNS_REPORT_NAME")
    else:
        report_name = geojson_file.split(os.path.sep)[-1].replace('.geojson', '').replace(' ', '_') + " - Riverscapes Inventory"

    args = [
        os.path.join(data_root, "rpt-riverscapes-inventory", report_name),
        geojson_file,
        report_name,
    ]
    if csv_file.strip():
        args.append("--csv")
        args.append(csv_file.strip())

    return args
