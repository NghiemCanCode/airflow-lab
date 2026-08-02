from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------------------------
# Python Functions
# ---------------------------------------------------------------------------

def _extract(**context):
    """Step 1: Giả lập lấy dữ liệu từ nguồn (API / Database)"""
    print("--- STEP 1: EXTRACTING DATA ---")
    raw_data = [
        {"user_id": 101, "name": "Alice", "score": 85, "region": "US"},
        {"user_id": 102, "name": "Bob", "score": -5, "region": "EU"},    # Dữ liệu lỗi
        {"user_id": 103, "name": "Charlie", "score": 92, "region": "US"},
        {"user_id": 104, "name": "David", "score": 78, "region": "APAC"},
    ]
    print(f"Extracted {len(raw_data)} records successfully.")
    context['ti'].xcom_push(key='raw_dataset', value=raw_data)


def _validate(**context):
    """Step 2: Lọc dữ liệu và đánh giá Quality Score"""
    print("--- STEP 2: VALIDATING DATA ---")
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data', key='raw_dataset')
    
    valid_data = []
    invalid_count = 0
    
    for row in raw_data:
        if row['score'] >= 0:
            valid_data.append(row)
        else:
            invalid_count += 1
            print(f"[WARNING] Dropped invalid record: {row}")

    ti.xcom_push(key='valid_dataset', value=valid_data)
    ti.xcom_push(key='invalid_count', value=invalid_count)
    print(f"Validation complete: {len(valid_data)} valid records, {invalid_count} dropped.")


def _quality_gate(**context):
    """Step 3: Branching Operator - Quyết định luồng đi tiếp"""
    ti = context['ti']
    invalid_count = ti.xcom_pull(task_ids='validate_data', key='invalid_count')
    
    if invalid_count > 2:
        print("[CRITICAL] Too many invalid records! Routing to Alert system.")
        return 'trigger_quality_alert'
    
    print("[SUCCESS] Data quality passed threshold. Proceeding to Transform.")
    return 'transform_us_data'


def _transform_by_region(region_name, **context):
    """Step 4 & 5: Transform song song theo vùng"""
    print(f"--- TRANSFORMING DATA FOR REGION: {region_name} ---")
    ti = context['ti']
    valid_data = ti.xcom_pull(task_ids='validate_data', key='valid_dataset')
    
    filtered = [r for r in valid_data if r['region'] == region_name]
    transformed = []
    
    for row in filtered:
        transformed.append({
            "user_id": row['user_id'],
            "name": row['name'].upper(),
            "score": row['score'],
            "grade": "PASS" if row['score'] >= 80 else "NEEDS_IMPROVEMENT",
            "processed_at": str(datetime.now())
        })
        
    print(f"Transformed {len(transformed)} records for {region_name}.")
    return transformed


def _simulate_db_load(**context):
    """Step 7: Load dữ liệu vào Target Storage"""
    # Comment this block for step 7
    # ====================================================
    print("--- STEP 7: LOADING DATA TO TARGET DB ---")
    print("Ingesting all transformed regional batches into Data Warehouse...")
    print("Data load complete!")

    # ====================================================
    # Uncomment this block for step 7
    # raise ConnectionError("Lost connection to Data Warehouse!")


def _cleanup(**context):
    """Step 10: Dọn dẹp temporary state"""
    print("--- STEP 10: CLEANUP TEMPORARY ASSETS ---")
    print("Temporary cache cleared. Workflow completed safely.")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

default_args = {
    'owner': 'senior_instructor',
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='dag_fundamentals_advanced',
    default_args=default_args,
    description='Lab 2: Complete 10-Step Advanced DAG Walkthrough',
    schedule='@daily',  # <--- ĐÃ SỬA THÀNH 'schedule' THAY VÌ 'schedule_interval'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lab', 'advanced', 'etl'],
) as dag:

    # Task 1: Extract
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=_extract,
    )

    # Task 2: Validate
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=_validate,
    )

    # Task 3: Quality Gate (Branching)
    task_gate = BranchPythonOperator(
        task_id='data_quality_gate',
        python_callable=_quality_gate,
    )

    # Branch phao cứu sinh
    task_alert = EmptyOperator(
        task_id='trigger_quality_alert'
    )

    # Task 4 & 5: Dynamic / Parallel Transformation
    task_transform_us = PythonOperator(
        task_id='transform_us_data',
        python_callable=_transform_by_region,
        op_kwargs={'region_name': 'US'}
    )

    task_transform_global = PythonOperator(
        task_id='transform_global_data',
        python_callable=_transform_by_region,
        op_kwargs={'region_name': 'APAC'}
    )

    # Task 6: Join / Merge Point
    task_join_transforms = EmptyOperator(
        task_id='join_transformed_data',
        trigger_rule='none_failed_min_one_success'
    )

    # Task 7: Load
    task_load = PythonOperator(
        task_id='load_data',
        python_callable=_simulate_db_load,
    )

    # Task 8 & 9: Audit & Notification
    task_audit = EmptyOperator(task_id='audit_log_metrics')
    task_notify = EmptyOperator(task_id='send_completion_slack_alert')

    # Task 10: Cleanup
    task_cleanup = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=_cleanup,
    )

    # ---------------------------------------------------------------------------
    # Dependency Graph Configuration
    # ---------------------------------------------------------------------------
    task_extract >> task_validate >> task_gate
    
    # Rẽ nhánh từ Gate
    task_gate >> task_alert
    task_gate >> [task_transform_us, task_transform_global] >> task_join_transforms
    
    # Sau khi Join -> Load
    task_join_transforms >> task_load
    
    # Load xong -> Chạy song song Audit & Notification
    task_load >> [task_audit, task_notify] >> task_cleanup