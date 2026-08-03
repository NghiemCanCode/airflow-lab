import os
import json
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Đường dẫn tới file Config bên ngoài (Tách biệt hoàn toàn khỏi logic xử lý)
CONFIG_FILE_PATH = "/opt/airflow/dags/config/pipeline_config.json"

default_args = {
    'owner': 'data_engineer_senior',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

@dag(
    dag_id='dag_dynamic_file_processor',
    default_args=default_args,
    description='Lab 5: Dynamic File Processing with External Config Decoupling',
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['dynamic_mapping', 'file_processing', 'decoupling', 'lab5'],
)
def dynamic_file_pipeline():

    # Task 1: Đọc Config ngoài & Quét thư mục tìm n file thực tế
    @task()
    def discover_incoming_files():
        """
        Đọc file config external, quét thư mục nguồn và trả về:
        - Danh sách n file hợp lệ
        - Các thông số config bổ sung
        """
        logging.info(f"Reading external configuration from: {CONFIG_FILE_PATH}")
        with open(CONFIG_FILE_PATH, 'r') as f:
            config = json.load(f)

        source_dir = config['source_directory']
        file_pattern = config['file_pattern']
        min_size = config.get('min_file_size_bytes', 0)

        logging.info(f"Scanning directory: '{source_dir}' for pattern '{file_pattern}'...")

        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"Source directory {source_dir} does not exist!")

        # Quét động toàn bộ file trong thư mục tại RUNTIME
        discovered_files = []
        for file_name in os.listdir(source_dir):
            full_path = os.path.join(source_dir, file_name)
            
            # Lọc theo đuôi file và kích thước file chuẩn
            if file_name.endswith(file_pattern) and os.path.getsize(full_path) >= min_size:
                discovered_files.append({
                    'file_path': full_path,
                    'file_name': file_name,
                    'target_table': config['target_table']
                })

        logging.info(f"SUCCESS: Discovered {len(discovered_files)} dynamic files matching criteria.")
        
        # Trả về danh sách n file tìm được
        return discovered_files

    # Task 2: Mapped Task - Xử lý từng file (Tự động nhân bản ra n tasks)
    @task()
    def process_single_file(file_info: dict) -> dict:
        """
        Xử lý 1 file duy nhất. Airflow sẽ tự nhân bản hàm này thành n tasks
        tương ứng với n file trong danh sách truyền vào qua .expand()
        """
        file_path = file_info['file_path']
        file_name = file_info['file_name']
        target_table = file_info['target_table']

        logging.info(f"--- [MAPPED TASK] Processing File: {file_name} ---")
        logging.info(f"Reading data from: {file_path}")

        # Giả lập đọc nội dung file CSV
        row_count = 0
        total_amount = 0.0

        with open(file_path, 'r') as f:
            lines = f.readlines()[1:] # Bỏ qua header
            row_count = len(lines)
            for line in lines:
                parts = line.strip().split(',')
                if len(parts) == 2:
                    total_amount += float(parts[1])

        logging.info(f"Successfully ingested {row_count} rows into target table '{target_table}'.")

        return {
            'file_name': file_name,
            'processed_rows': row_count,
            'total_amount': total_amount,
            'status': 'SUCCESS'
        }

    # Task 3: Gom kết quả tổng hợp từ n mapped tasks
    @task()
    def aggregate_pipeline_summary(processing_results: list[dict]):
        """
        Gom (Reduce) thông tin từ n tasks chạy song song về 1 điểm tổng kết chung.
        """
        logging.info("--- FINAL STEP: PIPELINE SUMMARY REPORT ---")
        
        total_files = len(processing_results)
        total_rows = sum(res['processed_rows'] for res in processing_results)
        grand_total_amount = sum(res['total_amount'] for res in processing_results)

        logging.info(f"Total Files Processed Today : {total_files}")
        logging.info(f"Total Records Ingested      : {total_rows} rows")
        logging.info(f"Grand Total Amount          : ${grand_total_amount:,.2f}")

    # =========================================================================
    # DYNAMIC TASK MAPPING FLOW (KHÔNG HARD-CODE N BẰNG TAY)
    # =========================================================================
    # Step 1: Quét n file
    file_list = discover_incoming_files()

    # Step 2: Nhân bản n task bằng .expand() tại RUNTIME
    mapped_jobs = process_single_file.expand(file_info=file_list)

    # Step 3: Gom báo cáo
    aggregate_pipeline_summary(mapped_jobs)

# Khởi tạo DAG
dynamic_file_pipeline()