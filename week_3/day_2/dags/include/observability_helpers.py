import logging
import time
from typing import Dict, Any
from airflow.models import TaskInstance

# Custom Logger dành riêng cho Observability Pipeline
obs_logger = logging.getLogger("airflow.task.observability")


def build_task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback thực thi khi Task gặp lỗi Fatal Failure (sau khi đã dùng hết số lần retry).
    Xuất ra Structured Log định dạng Dict/JSON để Log Collector thu thập.
    """
    ti: TaskInstance = context.get('task_instance')
    exception = context.get('exception')
    execution_date = context.get('logical_date')
    
    metric_data = {
        "event": "TASK_FAILURE",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "logical_date": str(execution_date),
        "try_number": ti.try_number,
        "duration_sec": round(ti.duration, 2) if ti.duration else 0.0,
        "exception_type": type(exception).__name__ if exception else "Unknown",
        "exception_reason": str(exception)
    }
    obs_logger.error(f"[STRUCTURED_METRIC] {metric_data}")


def build_task_retry_callback(context: Dict[str, Any]) -> None:
    """
    Callback thực thi mỗi khi Task thất bại và chuẩn bị bước vào trạng thái Retry.
    """
    ti: TaskInstance = context.get('task_instance')
    exception = context.get('exception')
    obs_logger.warning(
        f"[RETRY_EVENT] Task '{ti.task_id}' (DAG: {ti.dag_id}) failed on attempt {ti.try_number}. "
        f"Retrying... Root Reason: {exception}"
    )


def build_task_success_callback(context: Dict[str, Any]) -> None:
    """
    Callback thực thi khi Task hoàn thành thành công.
    """
    ti: TaskInstance = context.get('task_instance')
    duration = round(ti.duration, 2) if ti.duration else 0.0
    obs_logger.info(
        f"[STRUCTURED_METRIC] {{'event': 'TASK_SUCCESS', 'dag_id': '{ti.dag_id}', "
        f"'task_id': '{ti.task_id}', 'duration_sec': {duration}}}"
    )

def simulate_data_ingestion(latency_seconds: int) -> Dict[str, Any]:
    """
    Pure Logic: Giả lập quá trình đọc dữ liệu có Latency biến thiên.
    """
    obs_logger.info(f"Starting data ingestion with target latency: {latency_seconds}s")
    time.sleep(latency_seconds)
    return {"records_ingested": 1500, "latency_applied": latency_seconds}


def simulate_flaky_api_call(try_number: int, records_count: int) -> Dict[str, Any]:
    """
    Pure Logic: Giả lập API bên thứ ba bị chập chờn (Transient Error).
    Sẽ quăng lỗi HTTP 503 ở lần thử 1 và 2, thành công ở lần thử 3.
    """
    obs_logger.info(f"Executing Flaky API Call on attempt #{try_number}")
    if try_number < 3:
        obs_logger.error(f"API Error HTTP 503: Service Unavailable on attempt {try_number}")
        raise ConnectionResetError(f"HTTP 503 Connection dropped on try {try_number}")
    
    obs_logger.info("API Call Succeeded on attempt 3!")
    return {"status": "SUCCESS", "processed_records": records_count}


def simulate_data_validation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure Logic: Giả lập kiểm tra chất lượng dữ liệu gặp lỗi Deterministic Error (Lỗi logic dữ liệu).
    Chủ động ghi Log Record bị hỏng trước khi Raise Exception.
    """
    obs_logger.info("Starting data quality check...")
    invalid_record = {"user_id": None, "amount": "INVALID_NUMBER"}
    
    if invalid_record["user_id"] is None:
        obs_logger.critical(
            f"[DATA_QUALITY_FAILURE] Found null Primary Key! Record dump: {invalid_record}"
        )
        raise ValueError("Data Validation Failed: Primary Key 'user_id' cannot be Null.")
    return payload


def generate_pipeline_metric_summary(
    ingest_res: Any, 
    api_res: Any, 
    transform_res: Any, 
    dag_run: Any
) -> list:
    """
    Gom và in ra báo cáo tổng hợp kết quả của toàn bộ Pipeline.
    Thiết kế tương thích với Airflow 3.x Task SDK (Sử dụng Pydantic Context Model, không gọi DB ORM).
    """
    run_id = getattr(dag_run, 'run_id', 'UNKNOWN_RUN_ID')
    
    summary = [
        {
            "task_id": "ingest_data_with_latency",
            "status": "SUCCESS" if ingest_res else "FAILED/SKIPPED",
            "details": ingest_res
        },
        {
            "task_id": "call_flaky_payment_api",
            "status": "SUCCESS" if api_res else "FAILED/SKIPPED",
            "details": api_res
        },
        {
            "task_id": "validate_and_transform",
            "status": "SUCCESS" if transform_res else "FAILED",
            "details": transform_res or "Data Validation Error (See Logs)"
        }
    ]
    
    obs_logger.info(f"=================== METRIC SUMMARY REPORT (Run ID: {run_id}) ===================")
    for item in summary:
        obs_logger.info(
            f"TASK: {item['task_id']:<30} | STATUS: {item['status']:<15} | DETAILS: {item['details']}"
        )
    obs_logger.info("==========================================================================================")
    
    return summary