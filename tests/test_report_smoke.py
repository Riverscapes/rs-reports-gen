"""
Pytest-based smoke tests for Riverscapes report generators.
Launches each report CLI with example inputs and checks for expected output files.
I found the outputs in e.g. C:\Users\narlorin\AppData\Local\Temp\pytest-of-narlorin\pytest-10\test_report_smoke_watershed_su0
"""
import subprocess
import sys
import os
from pathlib import Path
import pytest

# List of reports to test: (script_name, example_input_dir, expected_outputs)
REPORTS = [
    {
        "name": "IGO Project",
        "module": "reports.rpt_igo_project.main",
        "example_dir": "src/reports/rpt_igo_project/example",
        "expected_files": ["README.md", "column_metadata.csv"],
        "construct_args": lambda module, inp, out: [
            sys.executable, "-m", module,
            os.environ.get("SPATIALITE_PATH", "MISSING_SPATIALITE"),
            str(out), str(inp), "TestProject"
        ],
        "requires_spatialite": True
    },
    {
        "name": "Rivers Need Space",
        "module": "reports.rpt_rivers_need_space.main",
        "example_dir": "src/reports/rpt_rivers_need_space/example",
        "expected_files": ["report.html", "data/data.csv", "data/data.xlsx"],
        "construct_args": lambda module, inp, out: [
            sys.executable, "-m", module,
            str(out), str(inp), "TestReport"
        ],
        "requires_spatialite": False
    },
    {
        "name": "watershed_summary_10digit",
        "module": "reports.rpt_watershed_summary.main",
        "example_dir": "src/reports/rpt_watershed_summary/example",
        "expected_files": ["report.html", "report.log"],
        "construct_args": lambda module, inp, out: [
            sys.executable, "-m", module, str(out), "1029010203", "TestWatershedReport"
        ],
    }
    # Add more reports as needed
]


@pytest.mark.parametrize("report", REPORTS, ids=lambda r: r["name"])
def test_report_smoke(report, tmp_path):
    """
    Launch report CLI with example input and check for expected output files.
    """
    # Check prerequisites
    if report.get("requires_spatialite") and not os.environ.get("SPATIALITE_PATH"):
        pytest.skip("SPATIALITE_PATH environment variable not set")

    example_dir = Path(report["example_dir"]).resolve()
    if not example_dir.is_dir():
        pytest.skip(f"Example dir not found: {example_dir}")

    # Find a geojson or other input file in the example dir
    input_files = list(example_dir.glob("*.geojson"))
    if not input_files:
        pytest.skip(f"No input files in {example_dir}")
    input_file = input_files[0]

    # Output directory
    output_dir = tmp_path

    # Build CLI args using the lambda
    cli_args = report["construct_args"](report["module"], input_file, output_dir)

    # Run the CLI
    # check=False is implicit but good to be explicit for clarity
    result = subprocess.run(cli_args, capture_output=True, text=True, check=False)

    # Check for success
    if result.returncode != 0:
        pytest.fail(f"{report['name']} failed (exit code {result.returncode}):\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")

    # Check for expected output files
    missing_files = []
    for fname in report["expected_files"]:
        fpath = output_dir / fname
        if not fpath.exists():
            missing_files.append(fname)

    if missing_files:
        found_files = [str(f.relative_to(output_dir)) for f in output_dir.rglob("*")]
        pytest.fail(f"Missing expected output files: {missing_files}\nFound the following files in output dir:\n{found_files}")
