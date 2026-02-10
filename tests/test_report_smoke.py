r"""
Pytest-based smoke tests for Riverscapes report generators.
Launches each report CLI with example inputs and checks for expected output files.
To run just one use -k and a word in the `name` 
 -r shows what is skipped -s show stdout/print statements -v verbose
`uv run python -m pytest -r -s -v .\tests\test_report_smoke.py -k "IGO"`
Environment variable TEST_ALL_EXAMPLES="true" to test every example, otherwise just picks the first one. 
Report outputs go in e.g. %temp%\pytest-of-narlorin\pytest-10\test_report_smoke_watershed_su0
"""
import subprocess
import sys
import os
from pathlib import Path
import pytest
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# List of reports to test: (script_name, example_input_dir, expected_outputs)
REPORTS = [
    {
        "name": "IGO Project",
        "module": "reports.rpt_igo_project.main",
        "example_dir": "src/reports/rpt_igo_project/example",
        "expected_files": ["README.md", "column_metadata.csv", "project.rs.xml", "outputs/riverscape_metrics.gpkg"],
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
        ]
    },
    {
        "name": "riverscapes_inventory",
        "module": "reports.rpt_riverscapes_inventory.main",
        "example_dir": "src/reports/rpt_riverscapes_inventory/example",
        "expected_files": ["report.html"],
        "construct_args": lambda module, inp, out: [
            sys.executable, "-m", module, str(out), str(inp), "riverscaps_inventory_test"
        ]
    },
    # Add more reports or configurations as needed
]


def get_test_cases():
    """Generate test cases based on found example input files."""
    cases = []
    # Check env var for overriding default behavior
    run_all = os.environ.get("TEST_ALL_EXAMPLES", "false").lower() == "true"

    for report in REPORTS:
        base_name = report["name"]
        example_dir = Path(report.get("example_dir", "")).resolve()

        found_files = []
        if example_dir.is_dir():
            found_files = sorted(list(example_dir.glob("*.geojson")))  # Sorted for deterministic order

        if not found_files:
            # Case: No files found. Yield a case that will Skip inside the test function
            cases.append(pytest.param(report, None, id=f"{base_name}-no_input", marks=pytest.mark.skip(reason=f"No input files found in {example_dir}")))
            continue

        # Determine which files to test
        files_to_test = found_files if run_all else [found_files[0]]

        for p in files_to_test:
            test_id = f"{base_name}-{p.name}"
            cases.append(pytest.param(report, p, id=test_id))

    return cases


@pytest.mark.parametrize("report,input_file", get_test_cases())
def test_report_smoke(report, input_file, tmp_path):
    """
    Launch report CLI with example input and check for expected output files.
    """
    if input_file is None:
        pytest.skip("No input file provided")

    # Check prerequisites
    if report.get("requires_spatialite") and not os.environ.get("SPATIALITE_PATH"):
        pytest.skip("SPATIALITE_PATH environment variable not set")

    # Output directory
    output_dir = tmp_path

    # Build CLI args using the lambda
    cli_args = report["construct_args"](report["module"], input_file, output_dir)

    # Print status for running with -s
    print(f"\n[TESTING] Report: {report['name']}")
    print(f"[TESTING] Input: {input_file.name if input_file else 'None'}")
    print(f"[TESTING] Command: {' '.join(str(x) for x in cli_args)}")
    sys.stdout.flush()

    # Run the CLI
    # We do NOT capture output so that it streams to the console (visible with -s).
    # This provides the "intermediate feedback" requesting during long runs.
    result = subprocess.run(cli_args, capture_output=False, text=True, check=False)

    # Check for success
    if result.returncode != 0:
        pytest.fail(f"{report['name']} failed (exit code {result.returncode}). See console output above for details.")
        fpath = output_dir / fname
        if not fpath.exists():
            missing_files.append(fname)

    if missing_files:
        found_files = [str(f.relative_to(output_dir)) for f in output_dir.rglob("*")]
        pytest.fail(f"Missing expected output files: {missing_files}\nFound the following files in output dir:\n{found_files}")
