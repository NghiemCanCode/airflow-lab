from datetime import datetime
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook

@dag(
    dag_id="dag_providers_and_connections",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["chapter_2", "lab_1", "connections"],
)
def pipeline_portable_lab():

    @task()
    def inspect_connection_metadata():
        conn = BaseHook.get_connection("postgres_warehouse")
        print(f"Conn ID: {conn.conn_id} | Host: {conn.host} | Port: {conn.port}")

    @task()
    def query_postgres_via_hook():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS lab_user_stats (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO lab_user_stats (username) VALUES ('airflow_user_dev');
        """
        pg_hook.run(create_table_sql)
        records = pg_hook.get_records("SELECT id, username FROM lab_user_stats LIMIT 5;")
        print(f"Fetched records via PostgresHook: {records}")

    @task()
    def upload_to_s3_minio():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook(aws_conn_id="minio_s3_conn")
        bucket_name = "airflow-lab-bucket"

        if not s3_hook.check_for_bucket(bucket_name):
            s3_hook.create_bucket(bucket_name=bucket_name)

        s3_hook.load_string(
            string_data="id,status\n1,SUCCESS\n2,PENDING",
            key="raw_data/orders.csv",
            bucket_name=bucket_name,
            replace=True
        )
        print("Uploaded raw_data/orders.csv to MinIO S3 successfully!")

    @task()
    def fetch_external_api():
        from airflow.providers.http.hooks.http import HttpHook
        http_hook = HttpHook(http_conn_id="HTTP_API_LAB", method="GET")
        response = http_hook.run(endpoint="todos/1")
        if response.status_code == 200:
            print(f"Fetched API Data: {response.json()}")

    # Dependency Flow
    inspect_connection_metadata() >> [query_postgres_via_hook(), upload_to_s3_minio(), fetch_external_api()]

pipeline_portable_lab()