import os
import inquirer


def main():
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

        SPATIALITE_PATH - path to the mod_spatialite library (REQUIRED)
        DATA_ROOT - Path to the outputs folder. A subfolder rpt-igo-project will be created if it does not exist (REQUIRED)

        IGO_AOI_GEOJSON - path to the input geojson file for rpt-igo-project (optional)
        IGO_REPORT_NAME - name for the report (optional)
    """
    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n")
    data_root = os.environ.get("DATA_ROOT")

    if not os.environ.get("SPATIALITE_PATH"):
        raise RuntimeError("\nSPATIALITE_PATH environment variable is not set. Please set it in your .env file\n\n  e.g.SPATIALITE_PATH=/opt/homebrew/lib/mod_spatialite.8.dylib")
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
        geojson_filename = geojson_question['geojson']
        geojson_file = os.path.abspath(os.path.join(base_dir, "example", geojson_filename))

    if os.environ.get("IGO_REPORT_NAME"):
        report_name = os.environ.get("IGO_REPORT_NAME")
    else:
        report_name = geojson_file.split(os.path.sep)[-1].replace('.geojson', '').replace(' ', '_') + " - IGO Scrape"

    # The final argument array we pass back
    return [
        spatialite_path,
        os.path.join(data_root, "rpt-igo-project", report_name),
        geojson_file,
        report_name,
    ]
