import time
import logging
from datetime import datetime
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

@dag(
    dag_id='c3_lab5_db_protection_pattern',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4, # Chỉ cho phép tối đa 4 batch chạy song song
    tags=['scaling', 'production', 'db_protection'],
)
def db_protection_pipeline():

    @task
    def fetch_large_dataset():
        # Giả sử có 100 bản ghi, thay vì trả 100 items riêng biệt
        # Ta chia nhỏ thành các Chunks (mỗi Chunk 20 items)
        raw_ids = list(range(100))
        chunk_size = 20
        chunks = [raw_ids[i:i + chunk_size] for i in range(0, len(raw_ids), chunk_size)]
        return chunks # Trả về 5 chunks -> Sinh ra đúng 5 tasks

    @task(pool="heavy_resource_pool")
    def process_batch(chunk: list):
        logger.info(f"Processing batch size {len(chunk)}: {chunk}")
        # Dùng 1 Connection duy nhất xử lý gọn 20 items trong chunk
        time.sleep(2)
        return f"Processed {len(chunk)} items"

    batches = fetch_large_dataset()
    process_batch.expand(chunk=batches)

db_protection_pipeline()