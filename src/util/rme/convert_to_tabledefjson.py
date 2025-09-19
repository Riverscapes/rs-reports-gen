import pandas as pd
import json

def csv_to_column_metadata_json(
    csv_path: str,
    json_path: str,
    extra_fields: dict = None
):
    """
    NOT USED - FOR NOW GOING FROM GOOGLE SHEETS TO CSV TO S3 TO ATHENA. LATER MAY USE THIS. 
    Convert a CSV of column definitions to a JSON array matching schema table_def.schema.json.
    Optionally, add extra fields (e.g., friendly_name, unit, description) as defaults.
    """
    df = pd.read_csv(csv_path)
    # Add extra fields if not present
    if extra_fields:
        for k, v in extra_fields.items():
            if k not in df.columns:
                df[k] = v

    # Ensure all required fields are present
    required = ["table_name", "name", "type"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Convert DataFrame to list of dicts
    records = df.to_dict(orient="records")

    # Write to JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(records)} column definitions to {json_path}")

# Example usage:
csv_to_column_metadata_json(
    "src/reports/rpt_igo_project/rme_table_column_defs.csv",
    "src/reports/rpt_igo_project/rme_table_column_defs.json",
    extra_fields={
        "friendly_name": "",
        "unit": "",
        "description": "",
        "category": "",
        "bins": None
    }
)