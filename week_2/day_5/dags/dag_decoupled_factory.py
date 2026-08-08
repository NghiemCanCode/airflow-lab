from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

EXECUTION_TARGET = Variable.get("EXECUTION_TARGET", default_var="DOCKER") # DOCKER / LOCAL

def get_etl_task(task_id: str, input_path: str, output_path: str, min_amount: float):
    """Factory method tạo task dựa trên cấu hình môi trường."""
    if EXECUTION_TARGET == "DOCKER":
        import os
        HOST_DATA_DIR = os.environ.get('HOST_DATA_DIR', '/home/ubuntu/airflow-lab/week_2/day_5/data')
        return DockerOperator(
            task_id=task_id,
            image="my-etl-job:v1.0",
            auto_remove="success",
            command=[
                "--input-path", input_path,
                "--output-path", output_path,
                "--min-amount", str(min_amount)
            ],
            mounts=[Mount(source=HOST_DATA_DIR, target="/data", type="bind")],
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mount_tmp_dir=False,
        )
    else:
        # Dự phòng chạy Local Python
        from etl_engine.cli import main as cli_main
        import sys
        
        def _local_runner():
            sys.argv = [
                "cli.py",
                "--input-path", f"/tmp/airflow_shared{input_path.replace('/app/data', '')}",
                "--output-path", f"/tmp/airflow_shared{output_path.replace('/app/data', '')}",
                "--min-amount", str(min_amount)
            ]
            cli_main()

        return PythonOperator(
            task_id=task_id,
            python_callable=_local_runner
        )

with DAG(
    dag_id="lab5_03_decoupled_factory_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    etl_task = get_etl_task(
        task_id="dynamic_etl_execution",
        input_path="/data/raw_orders.csv",
        output_path="/data/output/orders_factory.csv",
        min_amount=100.0
    )