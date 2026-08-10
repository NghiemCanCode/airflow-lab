import os
import yaml
import logging
from pendulum import datetime

# Airflow 3.3.0 Standard: Import decorators từ airflow.sdk
from airflow.sdk import dag, task
from include.etl_processor import DataETLProcessor

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "config/etl_config.yaml")

@dag(
    dag_id="dag_logging_config_testing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab6", "logging", "testing"]
)
def logging_config_testing_pipeline():

    @task
    def load_config_task() -> dict:
        logger.info("Đang đọc cấu hình từ file: %s", CONFIG_PATH)
        if not os.path.exists(CONFIG_PATH):
            logger.error("File cấu hình không tồn tại tại đường dẫn: %s", CONFIG_PATH)
            raise FileNotFoundError(f"Config not found at {CONFIG_PATH}")
            
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        
        logger.info("Đã load thành công config cho môi trường '%s'", config.get("app_env"))
        return config

    @task
    def run_etl_task(config: dict) -> list[dict]:
        dp_config = config.get("data_processing", {})
        
        processor = DataETLProcessor(
            min_score_threshold=dp_config.get("min_score_threshold", 0),
            bonus_multiplier=dp_config.get("bonus_multiplier", 1.0)
        )
        
        raw_dataset = [
            {"user_id": "USR_1001", "score": 50},
            {"user_id": "USR_1002", "score": -15},
            {"user_id": "USR_1003", "score": 100},
        ]
        
        return processor.process_records(raw_dataset)

    cfg = load_config_task()
    run_etl_task(cfg)

logging_config_testing_pipeline()