import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule

from include.helpers import process_single_partition_file


# ==============================================================================
# 1. DEFAULT ARGS DEFINITION
# ==============================================================================
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
}


# ==============================================================================
# 2. MAIN DAG DEFINITION
# ==============================================================================
@dag(
    dag_id='dag_production_pipeline_v1',
    default_args=default_args,
    description='Lab 6: End-to-End Best Practices & Mini Production Pipeline (Airflow 3.3.0)',
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['production', 'best_practices', 'lab6'],
)
def production_mini_pipeline():

    # --------------------------------------------------------------------------
    # TASK 1A: Fetch Configurations & Validate Target DB
    # --------------------------------------------------------------------------
    @task(task_id='fetch_configurations')
    def fetch_configurations() -> float:
        logging.info("--- [STEP 1A] FETCHING CONFIGURATIONS ---")

        config = Variable.get("lab6_pipeline_config", deserialize_json=True)
        logging.info(f"Loaded Variable Config: {config}")

        conn = BaseHook.get_connection("postgres_warehouse_conn")
        logging.info(f"Target DB metadata validated -> Host: {conn.host}:{conn.port}")

        return float(config.get("exchange_rate_usd_vnd", 25400))

    # --------------------------------------------------------------------------
    # TASK 1B: Discover Partition Files (Trả về list trực tiếp cho Dynamic Mapping)
    # --------------------------------------------------------------------------
    @task(task_id='discover_partition_files')
    def discover_files() -> list[str]:
        logging.info("--- [STEP 1B] DISCOVERING PARTITION FILES ---")

        config = Variable.get("lab6_pipeline_config", deserialize_json=True)
        source_dir = config["source_dir"]

        if not os.path.exists(source_dir):
            os.makedirs(source_dir, exist_ok=True)

        files = [
            os.path.join(source_dir, f) 
            for f in os.listdir(source_dir) 
            if f.endswith('.csv')
        ]

        logging.info(f"Discovered {len(files)} partition files for processing.")
        return files

    # --------------------------------------------------------------------------
    # TASK 2: Dynamic Mapped Processing Task
    # --------------------------------------------------------------------------
    @task(task_id='process_partition_file', multiple_outputs=True)
    def process_file(file_path: str, exchange_rate: float) -> dict:
        logging.info(f"--- [MAPPED TASK] Processing: {file_path} ---")
        result = process_single_partition_file(file_path, exchange_rate)
        return result

    # --------------------------------------------------------------------------
    # TASK 3: Downstream Audit & Consolidation Task
    # --------------------------------------------------------------------------
    @task(
        task_id='aggregate_and_audit',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    def consolidate_audit(processing_results: list[dict]):
        logging.info("--- [STEP 3] CONSOLIDATING AUDIT METRICS ---")

        # Lọc bỏ các giá trị None từ các mapped instances bị ngắt (nếu có)
        clean_results = [res for res in processing_results if res is not None]

        total_files = len(clean_results)
        total_valid = sum(res.get("valid_count", 0) for res in clean_results)
        total_errors = sum(res.get("error_count", 0) for res in clean_results)

        logging.info("==================================================")
        logging.info(" FINAL PIPELINE METRICS REPORT")
        logging.info(f" Total Partitions Processed : {total_files}")
        logging.info(f" Total Ingested Records    : {total_valid}")
        logging.info(f" Total Quarantined Errors   : {total_errors}")
        logging.info("==================================================")

    # --------------------------------------------------------------------------
    # WORKFLOW EXECUTION DEPENDENCIES
    # --------------------------------------------------------------------------
    rate = fetch_configurations()
    file_list = discover_files()

    mapped_jobs = process_file.partial(
        exchange_rate=rate
    ).expand(
        file_path=file_list  # file_list trả về trực tiếp XCom list -> Chuẩn 100%!
    )

    consolidate_audit(mapped_jobs)


# Khởi tạo DAG Instance
production_mini_pipeline()