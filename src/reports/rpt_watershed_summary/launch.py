import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf, get_unit_system


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

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))

    # IF we have everything we need from environment variables then we can skip the prompts

    # ── Unit system ───────────────────────────────────────────────────
    unit_system = get_unit_system()
    if unit_system is None:
        return None

    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()

    hucs = os.environ.get("WS_HUCS")
    if not hucs:
        hucs = questionary.text(message="HUC or comma separated list of HUCs to report on (HUC10 or bigger)", default="").ask()
        if hucs is None or len(hucs) == 0:
            print("\nNo HUC provided. Exiting.\n")
            return None

    # ── Report name ───────────────────────────────────────────────────
    report_name = os.environ.get("RWS_REPORT_NAME")
    if not report_name:
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
        "--unit_system",
        unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")

    return args
