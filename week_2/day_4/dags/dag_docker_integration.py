from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os


default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='dag_docker_integration',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lab_4', 'docker', 'portable'],
) as dag:
    # Đường dẫn tuyệt đối của thư mục 'data' trên MÁY HOST (Host OS)
    # Lưu ý: Vì dùng DooD, Docker Daemon chạy trên Host nên đường dẫn Mount 'source' phải là đường dẫn 
    # của HOST, không phải container Airflow Worker!
    HOST_DATA_DIR = os.environ.get('HOST_DATA_DIR', '/home/ubuntu/airflow-lab/week_2/day_4/data')

    run_etl_container = DockerOperator(
        task_id='run_polars_etl_container',
        image='my-etl-pipeline:v1.0',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
            'APP_ENV': 'production',
            'LOG_LEVEL': 'DEBUG',
            'EXECUTION_TIMESTAMP': '{{ ts }}'
        },
        mounts=[
            Mount(
                source=HOST_DATA_DIR,
                target='/data',
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        command=[
            "--date", "{{ ds }}",
            "--input", "/data/raw_orders.csv",
            "--output", "/data/output/processed_{{ ds }}.csv"
        ],
    )

    run_etl_failing = DockerOperator(
        task_id='run_etl_failing_test',
        image='my-etl-pipeline:v1.0',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(
                source=HOST_DATA_DIR,
                target='/data',
                type='bind'
            )
        ],
        command=[
            "--date", "{{ ds }}",
            "--input", "/data/non_existent_file.csv", # File không tồn tại
            "--output", "/data/output/should_fail.csv"
        ],
        mount_tmp_dir=False
    )

    run_etl_container >> run_etl_failing
