from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Cấu hình Default Args cho Retry
default_args = {
    'owner': 'senior_instructor',
    'retries': 2,                           # Cho phép thử lại tối đa 2 lần
    'retry_delay': timedelta(seconds=15),   # Khoảng chờ giữa các lần retry là 15s
}

def _log_scheduler_timestamps(**context):
    """Phân tích sâu về thời gian thực thi của Scheduler"""
    print("=== [SCHEDULER TIMING ANALYSIS] ===")
    print(f"1. Logical Date (Execution Date) : {context['logical_date']}")
    print(f"2. Data Interval Start           : {context['data_interval_start']}")
    print(f"3. Data Interval End             : {context['data_interval_end']}")
    print(f"4. Actual Clock Time (Wall Time) : {datetime.now()}")
    print("====================================")

def _simulate_flaky_api(**context):
    """
    Giả lập API mạng/DB chập chờn:
    Lần 1 & Lần 2 cố tình nổ lỗi.
    Lần 3 (Retry lần 2) sẽ TỰ ĐỘNG THÀNH CÔNG mà không cần sửa code!
    """
    ti = context['ti']
    # try_number phản ánh lần chạy hiện tại
    print(f"--- [TASK ATTEMPT] Current Try Number: {ti.try_number} ---")
    
    if ti.try_number < 3:
        print(f"[TRANSIENT ERROR] Network timeout on attempt #{ti.try_number}. Throwing exception...")
        raise ConnectionError(f"Simulated Network Flakiness on attempt #{ti.try_number}")
    
    print(f"[SUCCESS] Network re-established on attempt #{ti.try_number}! Data fetched successfully.")

def _heavy_task_delay(**context):
    """Tạo độ trễ nhẹ để kịp quan sát trạng thái RUNNING trên UI"""
    print("Task is processing... Sleep for 5 seconds.")
    time.sleep(5)

# Định nghĩa DAG
with DAG(
    dag_id='dag_scheduling_and_lifecycle',
    default_args=default_args,
    description='Lab 3: Scheduler Timestamps, Automated Retries, and Task Lifecycle',
    schedule='*/5 * * * *',                   # CHẠY TỰ ĐỘNG MỖI 5 PHÚT
    start_date=datetime(2026, 8, 1),
    catchup=False,                            # Không chạy bù lịch sử
    max_active_runs=1,                        # Chỉ cho phép 1 DAG Run hoạt động cùng lúc
) as dag:

    # Task 1: Phân tích Timestamps
    analyze_timing = PythonOperator(
        task_id='analyze_scheduler_timing',
        python_callable=_log_scheduler_timestamps,
    )

    # Task 2: Task chạy tốn thời gian để soi trạng thái Running
    heavy_processing = PythonOperator(
        task_id='heavy_processing_task',
        python_callable=_heavy_task_delay,
    )

    # Task 3: Task chập chờn (Flaky) tự phục hồi nhờ Retry
    flaky_api_call = PythonOperator(
        task_id='flaky_api_call',
        python_callable=_simulate_flaky_api,
    )

    # Task 4: Báo động khẩn cấp (Chỉ kích hoạt NẾU có task bị Fail)
    # Khác với Lab 2 (dùng alert khi branch rớt vào điều kiện), ở đây dùng Trigger Rule chuyên biệt
    failure_alert = EmptyOperator(
        task_id='send_urgent_incident_alert',
        trigger_rule='one_failed',
    )

    # Task 5: Dọn dẹp tài nguyên (Luôn luôn chạy bất kể thành công hay thất bại)
    always_cleanup = EmptyOperator(
        task_id='always_cleanup_resources',
        trigger_rule='all_done',
    )

    # Luồng công việc (Dependencies)
    analyze_timing >> heavy_processing >> flaky_api_call
    flaky_api_call >> failure_alert >> always_cleanup
    flaky_api_call >> always_cleanup