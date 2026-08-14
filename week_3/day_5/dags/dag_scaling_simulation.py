import time
import logging
from datetime import datetime
# Cú pháp chuẩn Airflow 3.3.0 Task SDK
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'senior_dev',
    'retries': 0,
}

@dag(
    dag_id='c3_lab5_scaling_and_executor',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=16, # Tham số chuẩn cấp DAG trong Airflow 3 (khống chế 16 tasks/DAG)
    tags=['scaling', 'production', 'airflow3'],
)
def scaling_simulation_pipeline():

    @task(task_id="generate_workload")
    def generate_payload():
        # Sinh ra 60 items giả lập workload
        return [f"payload_item_{i}" for i in range(60)]

    @task(
        task_id="process_heavy_task",
        pool="heavy_resource_pool", # Ép task chui vào pool 5 slots
        pool_slots=1,
    )
    def process_item(item_id: str):
        logger.info(f"--- [START] Processing {item_id} ---")
        time.sleep(4) # Giả lập I/O hoặc tính toán heavy
        logger.info(f"--- [END] Processed {item_id} ---")
        return item_id

    @task(task_id="aggregate_results")
    def aggregate(results: list):
        logger.info(f"Successfully processed {len(results)} items.")

    payloads = generate_payload()
    processed = process_item.expand(item_id=payloads)
    aggregate(processed)

# Khởi tạo DAG
scaling_simulation_pipeline()