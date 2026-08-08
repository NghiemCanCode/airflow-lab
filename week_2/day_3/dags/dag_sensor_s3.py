from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.decorators import task

with DAG(
    dag_id='lab3_4_sensor_s3_guardrails',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 1. Chờ Object trên MinIO
    # 2. Áp dụng Exponential Backoff để tránh spam HTTP Request
    # 3. Áp dụng soft_fail=True phòng ngừa lỗi gián đoạn pipeline
    wait_minio_file = S3KeySensor(
        task_id='wait_for_minio_orders',
        bucket_name='warehouse',
        bucket_key='incoming/orders_data.csv',
        aws_conn_id='minio_s3_conn',   # Connection MinIO đã tạo ở Bước 1
        poke_interval=5,              # Thời gian check ban đầu
        exponential_backoff=True,      # Nhân đôi thời gian chờ sau mỗi lần poke thất bại (5s -> 10s -> 20s...)
        max_wait=60,                   # Giới hạn interval tối đa
        timeout=30,                    # Hết 30 giây tổng thời gian sẽ ngắt
        soft_fail=True,                # Không Fail task, chuyển state thành SKIPPED nếu hết timeout
        deferrable=True,               # Kết hợp bất đồng bộ bất kể là S3/MinIO
    )

    @task
    def process_minio_data():
        print("Đã phát hiện file trên MinIO Bucket!")

    wait_minio_file >> process_minio_data()