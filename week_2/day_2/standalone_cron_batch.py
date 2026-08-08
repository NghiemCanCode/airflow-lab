import csv
import sys
from pathlib import Path

# Import thẳng module logic mà KHÔNG CẦN Airflow context
from dags.include.business_logic.order_processor import (
    export_to_json_atomically,
    transform_records,
    validate_records,
)

def run_standalone_job(input_csv: str, output_json: str):
    print(f"[Standalone Job] Starting processing {input_csv}...")
    
    with open(input_csv, "r", encoding="utf-8") as f:
        raw_data = list(csv.DictReader(f))

    valid, invalid = validate_records(raw_data)
    transformed = transform_records(valid)
    
    export_to_json_atomically(transformed, output_json, "./tmp_staging")
    print(f"[Standalone Job] Complete! Exported to {output_json}")

if __name__ == "__main__":
    run_standalone_job(
        input_csv="dags/include/data/input/raw_orders.csv",
        output_json="standalone_output.json"
    )