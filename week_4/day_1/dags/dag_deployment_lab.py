import os
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

ENV = os.getenv("AIRFLOW_ENV", "DEV").upper()
BUCKET_NAME = os.getenv("MINIO_BUCKET", "dev-data")

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="c4_lab1_deployment_strategy",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["deployment", ENV],
)
def deployment_pipeline():

    @task
    def check_environment_guardrails():
        logger.info(f"=== RUNNING IN ENVIRONMENT: {ENV} ===")
        logger.info(f"Target Bucket: {BUCKET_NAME}")
        
        if ENV == "DEV":
            logger.info("🛡️ [GUARDRAIL]: Sampling 10% data for DEV.")
            return {"sample_ratio": 0.1, "env": "DEV"}
        elif ENV == "PROD":
            logger.info("🚀 [PRODUCTION]: Processing 100% full dataset.")
            return {"sample_ratio": 1.0, "env": "PROD"}
        else:
            raise ValueError(f"Unknown Environment: {ENV}")

    @task
    def generate_and_transform_data(config: dict):
        rows = 1000 if config["env"] == "PROD" else 100
        df = pd.DataFrame({
            "order_id": range(1, rows + 1),
            "amount": [100.0 * i for i in range(1, rows + 1)],
            "environment": config["env"],
            "processed_at": datetime.now().isoformat()
        })
        
        file_path = f"/tmp/processed_data_{config['env']}.csv"
        df.to_csv(file_path, index=False)
        return file_path

    @task
    def upload_to_storage(file_path: str):
        s3_hook = S3Hook(aws_conn_id="MINIO_S3_CONN")
        s3_key = f"exports/{datetime.now().strftime('%Y%m%d')}/{os.path.basename(file_path)}"
        
        logger.info(f"Uploading {file_path} to S3 Bucket '{BUCKET_NAME}' key '{s3_key}'...")
        s3_hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        logger.info("Upload completed successfully!")

        #======================================================================
        # Uncomment the following block for step 9
        #======================================================================

        # raise Exception("Just bug") 

            

    env_config = check_environment_guardrails()
    data_file = generate_and_transform_data(env_config)
    upload_to_storage(data_file)



deployment_pipeline()