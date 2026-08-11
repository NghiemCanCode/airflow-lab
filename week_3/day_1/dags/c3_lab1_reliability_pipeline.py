from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from include.exceptions import TransientError, FatalDataError
from include.unreliable_service import ExternalDataService
from include.callbacks import (
    on_task_failure_callback,
    on_task_retry_callback,
    on_pipeline_success_callback,
)

default_args = {
    'owner': 'data_engineering',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),  # Lần 1 chờ 5s
    'retry_exponential_backoff': True,    # Lần 2 chờ 10s, Lần 3 chờ 20s
    'max_retry_delay': timedelta(seconds=60),
    'on_failure_callback': on_task_failure_callback,
    'on_retry_callback': on_task_retry_callback,
}

@dag(
    dag_id='c3_lab1_reliability_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    on_success_callback=on_pipeline_success_callback,
    tags=['production', 'reliability', 'lab1'],
)
def reliability_pipeline():
    @task(task_id="extract_api_data")
    def extract_data():
        context = get_current_context()
        ti = context['ti']
        
        print(f"--- [EXTRACT] Bắt đầu gọi API (Attempt hiện tại: {ti.try_number}) ---")
        # ExternalDataService sẽ throw TransientError ở lần 1 và 2
        data = ExternalDataService.fetch_api_data(try_number=ti.try_number)
        return data

    @task(task_id="transform_data")
    def transform_data(raw_data: list):
        context = get_current_context()
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        trigger_fatal = conf.get('trigger_fatal', False)
        
        try:
            print(f"--- [TRANSFORM] Bắt đầu xử lý dữ liệu (trigger_fatal={trigger_fatal}) ---")
            return ExternalDataService.process_data(raw_data, fail_mode=trigger_fatal)
        except FatalDataError as e:
            print(f"❌ [CIRCUIT BREAKER] Phát hiện lỗi vĩnh viễn: {e}. Ngắt mạch lập tức!")
            # AirflowFailException báo hiệu cho Scheduler bỏ qua toàn bộ retries còn lại
            raise AirflowFailException(e)

    @task(task_id="cleanup_resources", trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_resources():
        print("🧹 [CLEANUP] Đang giải phóng các kết nối DB và xóa file tạm...")
        print("🧹 [CLEANUP] Hệ thống đã sẵn sàng cho DAG Run tiếp theo.")

    # Luồng thực thi
    raw = extract_data()
    transformed = transform_data(raw)
    transformed >> cleanup_resources()

reliability_pipeline_dag = reliability_pipeline()