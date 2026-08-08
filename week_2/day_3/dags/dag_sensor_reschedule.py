from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task

DROPZONE_PATH = "/opt/airflow/dags/data_dropzone/incoming_data.csv"

with DAG(
    dag_id='lab3_3_sensor_deferrable_local',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # FileSensor chạy ở chế độ Deferrable trên local storage
    wait_file_async = FileSensor(
        task_id='wait_for_local_file_async',
        filepath=DROPZONE_PATH,
        fs_conn_id='fs_default',   # Dùng Connection local đã tạo ở Bước 1
        poke_interval=5,
        timeout=300,
        deferrable=True,            # Đẩy việc chờ từ Worker xuống airflow-triggerer
    )

    @task
    def process_local_file():
        print(f"File local tại {DROPZONE_PATH} đã xuất hiện!")

    wait_file_async >> process_local_file()