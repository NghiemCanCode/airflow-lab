from datetime import datetime, timedelta
from airflow.decorators import dag, task
from include.business_logic.metrics import calculate_daily_revenue
import pandas as pd
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_platform_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='dag_ci_cd_demo',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['production', 'ci_cd_validated']
)
def ci_cd_pipeline():

    @task()
    def extract_data() -> list:
        # Giả lập extract data
        raw_data = [
            {'order_id': 1, 'amount': 150.0, 'status': 'COMPLETED'},
            {'order_id': 2, 'amount': -50.0, 'status': 'COMPLETED'}, # Invalid amount
            {'order_id': 3, 'amount': 200.0, 'status': 'FAILED'},
            {'order_id': 4, 'amount': 300.0, 'status': 'COMPLETED'},
        ]
        return raw_data

    @task()
    def process_revenue(raw_data: list) -> float:
        df = pd.DataFrame(raw_data)
        revenue = calculate_daily_revenue(df)
        logger.info(f"Calculated daily revenue: {revenue}")
        return revenue

    # Pipeline dependency
    data = extract_data()
    process_revenue(data)

dag_obj = ci_cd_pipeline()