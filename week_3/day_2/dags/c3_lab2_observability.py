# dags/c3_lab2_observability.py
import random
from datetime import timedelta
from pendulum import datetime
from airflow.decorators import dag, task

# Import các helpers từ module dags/include/
from include.observability_helpers import (
    build_task_failure_callback,
    build_task_retry_callback,
    build_task_success_callback,
    simulate_data_ingestion,
    simulate_flaky_api_call,
    simulate_data_validation,
    generate_pipeline_metric_summary
)

default_args = {
    'owner': 'data_ops',
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': build_task_failure_callback,
    'on_retry_callback': build_task_retry_callback,
    'on_success_callback': build_task_success_callback,
}

@dag(
    dag_id='c3_lab2_logging_monitoring_observability',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['production', 'observability', 'lab2']
)
def observability_pipeline():

    @task(execution_timeout=timedelta(seconds=10))
    def ingest_data_with_latency():
        # Chọn ngẫu nhiên thời gian chạy 2s, 3s, 5s hoặc 6s
        processing_time = random.choice([2, 3, 5, 6])
        return simulate_data_ingestion(latency_seconds=processing_time)

    @task(
        retries=3,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=10)
    )
    def call_flaky_payment_api(ingest_info: dict, **kwargs):
        ti = kwargs['ti']
        return simulate_flaky_api_call(
            try_number=ti.try_number, 
            records_count=ingest_info["records_ingested"]
        )

    @task(retries=0)
    def validate_and_transform(payload: dict):
        return simulate_data_validation(payload=payload)

    @task(trigger_rule='all_done')
    def export_pipeline_metrics(ingest_res, api_res, transform_res, **kwargs):
        dag_run = kwargs['dag_run']
        return generate_pipeline_metric_summary(
            ingest_res=ingest_res,
            api_res=api_res,
            transform_res=transform_res,
            dag_run=dag_run
        )

    
    # Wiring Pipeline
    ingest_out = ingest_data_with_latency()
    api_out = call_flaky_payment_api(ingest_out)
    transform_out = validate_and_transform(api_out)
    
    export_pipeline_metrics(ingest_out, api_out, transform_out)

dag_obj = observability_pipeline()