from datetime import datetime, timedelta
import logging
from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.log import mask_secret
from include.security.runtime_injector import get_ephemeral_db_connection

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'security_team',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

@dag(
    dag_id='c3_lab3_secrets_and_security',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['security', 'vault', 'production']
)
def secrets_management_pipeline():

    @task
    def test_secret_masking_mechanisms() -> str:
        # --- CƠ CHẾ 1: Automatic Masking ---
        # Lấy giá trị từ Vault. Airflow tự động đăng ký chuỗi giá trị này vào SensitiveDataMasker
        api_key = Variable.get("payment_api_key")
        logger.info("[Auto-Mask Test] Secret fetched from Vault: %s", api_key)

        # --- CƠ CHẾ 2: Manual Masking ---
        # Giả sử tự sinh Ephemeral Token tại runtime (không nằm trong Vault)
        generated_jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.generated_runtime_secret_999"
        
        # BẮT BUỘC gọi mask_secret TRƯỚC khi thực hiện logging
        mask_secret(generated_jwt_token)
        logger.info("[Manual-Mask Test] Generated Ephemeral Token: %s", generated_jwt_token)

        #################################################################
        # Cố tình trả về secret để thử nghiệm bẫy XCom ở Bước 7
        # return api_key
        #################################################################

        # CHỈ TRẢ VỀ METADATA (Tránh XCom Security Leakage)
        return {"status": "SUCCESS", "masking_test": "PASSED"}

    @task
    def execute_query_with_runtime_injection():
        # Lấy connection đã qua Runtime Injection
        conn_obj = get_ephemeral_db_connection(
            conn_id="warehouse_postgres", 
            dynamic_schema="airflow"
        )
        
        logger.info("Connecting to Host: %s, Database: %s, User: %s", 
                    conn_obj.host, conn_obj.schema, conn_obj.login)
        
        # Password lấy từ connection object cũng tự động bị Auto-Mask
        logger.info("Extracted Password from Connection: %s", conn_obj.password)

        # Thực thi qua PostgresHook bằng Connection Object trong memory
        hook = PostgresHook(connection=conn_obj)
        db_version = hook.get_first("SELECT version();")
        logger.info("Successfully executed query on Database Version: %s", db_version[0])

        return {"status": "SUCCESS", "db_version": db_version[0]}

    # ===============================================================
    # Uncomment the following code block for lab 8
    # ===============================================================
    #################################################################
    @task
    def secure_task_execution():
        """
        FETCH AT RUNTIME - USE IN MEMORY - DISCARD IMMEDIATELY
        """
        # Secret chỉ sống trong bộ nhớ RAM thuộc Scope của hàm này
        api_key = Variable.get("payment_api_key")
        conn_obj = get_ephemeral_db_connection("warehouse_postgres")
        
        hook = PostgresHook(connection=conn_obj)
        db_version = hook.get_first("SELECT version();")
        
        logger.info("Task processing completed securely. Executed on DB: %s", db_version[0])
        
        # CHỈ trả về Metadata/Status qua XCom, KHÔNG trả về Secret
        return {"status": "SUCCESS", "records_processed": 100}
    #################################################################

    # Flow
    # raw_key = test_secret_masking_mechanisms()
    # execute_query_with_runtime_injection(raw_key)

    # ===============================================================
    # Uncomment the following code block for lab 8
    # ===============================================================
    ####################################
    task_masking = test_secret_masking_mechanisms()
    task_execution = execute_query_with_runtime_injection()

    task_masking >> task_execution
    ####################################

secrets_management_pipeline()