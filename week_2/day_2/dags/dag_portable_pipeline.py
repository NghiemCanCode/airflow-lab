import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from include.business_logic.order_processor import (
    export_to_json_atomically,
    transform_records,
    validate_records,
)

BASE_DIR = Path("/opt/airflow/dags/include/data")

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}


@dag(
    dag_id="c2_lab2_data_pipeline_design",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["chapter2", "idempotent", "portable"],
)
def data_pipeline_dag():

    @task
    def read_and_validate_input() -> dict:
        input_file = BASE_DIR / "input" / "raw_orders.csv"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found at {input_file}")

        with open(input_file, "r", encoding="utf-8") as f:
            raw_data = list(csv.DictReader(f))

        valid_data, invalid_data = validate_records(raw_data)
        logging.info(f"Parsed {len(raw_data)} records. Valid: {len(valid_data)}, Invalid: {len(invalid_data)}")
        
        return {"valid": valid_data, "invalid_count": len(invalid_data)}

    @task
    def transform_data(validated_payload: dict) -> list[dict]:
        valid_records = validated_payload["valid"]
        transformed = transform_records(valid_records, exchange_rate_eur=1.1)
        return transformed

    @task
    def export_data(transformed_records: list[dict], **kwargs) -> str:
        # Lấy logical_date dạng YYYY-MM-DD để tạo tên file deterministic
        logical_date_str = kwargs["logical_date"].strftime("%Y-%m-%d")
        
        output_path = str(BASE_DIR / "output" / f"orders_processed_{logical_date_str}.json")
        staging_dir = str(BASE_DIR / "staging")

        result_path = export_to_json_atomically(
            data=transformed_records,
            target_path=output_path,
            staging_dir=staging_dir,
        )
        return result_path

    # Định nghĩa Workflow
    validated_payload = read_and_validate_input()
    transformed_records = transform_data(validated_payload)
    export_data(transformed_records)


data_pipeline_dag()