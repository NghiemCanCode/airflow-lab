from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Cấu hình default_args chuẩn
default_args = {
    'owner': 'senior_instructor',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# 1. Khai báo DAG bằng Decorator @dag (KHÔNG dùng with DAG(...) as dag)
@dag(
    dag_id='dag_taskflow_xcom',
    default_args=default_args,
    description='Lab 4: Modern TaskFlow API and Automatic XCom Management',
    schedule=None,                           # Trigger thủ công để dễ quan sát
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['taskflow', 'xcom_deep_dive', 'lab4'],
)
def taskflow_etl_pipeline():

    # 2. Task 1: Extract Data
    @task()
    def extract() -> list[dict]:
        """Giả lập lấy dữ liệu thô từ nguồn External API/DB"""
        print("--- STEP 1: EXTRACTING RAW DATA ---")
        raw_dataset = [
            {"order_id": "ORD_001", "user_id": 101, "amount": 150.0, "status": "COMPLETED"},
            {"order_id": "ORD_002", "user_id": 102, "amount": -50.0, "status": "INVALID"}, # Dữ liệu lỗi
            {"order_id": "ORD_003", "user_id": 103, "amount": 320.5, "status": "COMPLETED"},
            {"order_id": "ORD_004", "user_id": 104, "amount": 0.0,   "status": "PENDING"},
        ]
        print(f"Extracted {len(raw_dataset)} raw records.")
        # Dữ liệu return ở đây sẽ TỰ ĐỘNG biến thành XCom với key='return_value'
        return raw_dataset

    # 3. Task 2: Validate Data (Dùng multiple_outputs=True)
    @task(multiple_outputs=True)
    def validate(raw_data: list[dict]) -> dict:
        """
        Kiểm tra chất lượng dữ liệu.
        Trả về Dictionary để Airflow tự tách thành nhiều XCom Keys riêng biệt.
        """
        print("--- STEP 2: VALIDATING DATA & QUALITY CHECK ---")
        valid_orders = []
        rejected_orders = []

        for row in raw_data:
            if row['amount'] > 0 and row['status'] != 'INVALID':
                valid_orders.append(row)
            else:
                rejected_orders.append(row)

        print(f"Validation passed: {len(valid_orders)} valid, {len(rejected_orders)} rejected.")
        
        # Nhờ multiple_outputs=True, Dict này sẽ tự động phân tách thành 3 XCom keys:
        # 'valid_dataset', 'rejected_dataset', 'quality_score'
        return {
            'valid_dataset': valid_orders,
            'rejected_dataset': rejected_orders,
            'quality_score': (len(valid_orders) / len(raw_data)) * 100
        }

    # 4. Task 3: Transform Data
    @task()
    def transform(valid_data: list[dict]) -> list[dict]:
        """Tẩy rửa và tính toán giá trị gia tăng (USD -> VND giả lập)"""
        print("--- STEP 3: TRANSFORMING VALID DATA ---")
        transformed_records = []
        usd_to_vnd_rate = 25000

        for item in valid_data:
            transformed_records.append({
                "order_id": item['order_id'],
                "user_id": item['user_id'],
                "amount_usd": item['amount'],
                "amount_vnd": item['amount'] * usd_to_vnd_rate,
                "processed_at": str(datetime.now())
            })

        print(f"Transformed {len(transformed_records)} records successfully.")
        return transformed_records

    # 5. Task 4: Load Data & Audit Logging
    @task()
    def load(transformed_data: list[dict], quality_score: float):
        """
        Nhận dữ liệu đã transform VÀ chất lượng dữ liệu để ghi vào Target Storage.
        Task này nhận đầu vào từ 2 task khác nhau!
        """
        print("--- STEP 4: LOADING DATA & RECORDING METRICS ---")
        print(f"Data Quality Score received: {quality_score:.2f}%")
        print(f"Ingesting {len(transformed_data)} clean records into Data Warehouse...")
        
        for record in transformed_data:
            print(f"  -> Ingested Order: {record['order_id']} | VND: {record['amount_vnd']:,}")
            
        print("Load completed successfully!")

    # =========================================================================
    # IMPLICIT DEPENDENCY GRAPH (Luồng thực thi tự động không cần dấu >>)
    # =========================================================================
    # Task 1: Extract
    raw_data = extract()

    # Task 2: Validate (Truyền raw_data trực tiếp làm tham số)
    validation_results = validate(raw_data)

    # Task 3: Transform (Chỉ lấy 'valid_dataset' từ kết quả validate)
    transformed_data = transform(validation_results['valid_dataset'])

    # Task 4: Load (Kết hợp 'transformed_data' và 'quality_score' từ Task 2)
    load(
        transformed_data=transformed_data,
        quality_score=validation_results['quality_score']
    )

# Khởi tạo pipeline
taskflow_etl_pipeline()