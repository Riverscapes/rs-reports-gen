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
            WS_HUCS - HUC or HUCs (comma separated) to process 
            WS_REPORT_NAME - name for the report (optional)

    """

    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))
    data_root = os.environ.get("DATA_ROOT", ".")

    # IF we have everything we need from environment variables then we can skip the prompts

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

    if os.environ.get("WS_HUCS"):
        hucs = os.environ.get("WS_HUCS")
    else:

        huc_list_question = inquirer.prompt([
            inquirer.Text('hucs',
                          message="HUC or comma separated list of HUCs to report on (HUC10 or bigger)",
                          default=""
                          )
        ])
        if not huc_list_question or 'hucs' not in huc_list_question or len(huc_list_question.get('hucs')) == 0:
            print("\nNo HUC provided. Exiting.\n")
            return None
        hucs = huc_list_question.get('hucs')

    if os.environ.get("RSI_REPORT_NAME"):
        report_name = os.environ.get("RSI_REPORT_NAME")
    else:
        # build a report name from the HUCs provided
        huc_list2 = hucs.split(",", 2)
        report_name = 'HUC ' + huc_list2[0][:10]
        if len(huc_list2) > 1:
            report_name += " and others"

    # Create a clean, combined folder name for the report output
    report_folder_name = f"{report_name[:50].replace(' ', '_')}_{unit_system}"

    args = [
        Path(data_root) / "rpt-watershed-summary" / report_folder_name,
        hucs,
        report_name,
        "--unit_system", unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")

    return args
