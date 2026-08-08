from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import os

DROPZONE_PATH = "/opt/airflow/dags/data_dropzone/incoming_data.csv"

default_args = {
    'owner': 'data_engineer',
    'retries': 0,
}

with DAG(
    dag_id='lab3_1_sensor_poke_blocking',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_incoming_file',
        filepath=DROPZONE_PATH,
        poke_interval=5,      # Kiểm tra mỗi 5 giây
        timeout=60,           # Hết 60 giây nếu không thấy file sẽ Fail
        mode='poke',          # Mặc định: Giữ nguyên Worker Slot
    )

    def process_file():
        print(f"Đã thấy file tại {DROPZONE_PATH}. Bắt đầu đọc file...")

    read_file = PythonOperator(
        task_id='read_and_process_file',
        python_callable=process_file,
    )

    wait_for_file >> read_file