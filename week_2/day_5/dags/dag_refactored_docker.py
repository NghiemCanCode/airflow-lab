from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="lab5_02_docker_operator_decoupled",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_etl_container = DockerOperator(
        task_id="run_etl_docker",
        image="my-etl-job:v1.0",
        api_version="auto",
        auto_remove="success", # Xóa container sau khi chạy xong thành công để tiết kiệm disk
        command=[
            "--input-path", "/app/data/input/orders.csv",
            "--output-path", "/app/data/output/orders_docker.csv",
            "--min-amount", "100.0"
        ],
        mounts=[
            Mount(
                source="/tmp/airflow_shared", # Thư mục trên máy Host
                target="/app/data",           # Thư mục bên trong Container my-etl-job
                type="bind"
            )
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )