import logging

logger = logging.getLogger(__name__)

def on_task_failure_callback(context):
    """Kích hoạt khi Task bị FAILED hẳn (đã dùng hết toàn bộ lượt retry)"""
    ti = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    exception = context.get('exception')
    log_url = ti.log_url
    
    msg = f"""
    ❌ [ALERT] TASK FAILED PERMANENTLY!
    -------------------------------------------
    DAG: {dag_id}
    Task: {ti.task_id}
    Logical Date: {context.get('logical_date')}
    Attempt: {ti.try_number} / {ti.max_tries + 1}
    Error: {exception}
    Log URL: {log_url}
    -------------------------------------------
    """
    logger.error(msg)

def on_task_retry_callback(context):
    """Kích hoạt MỖI LẦN Task bị lỗi và chuẩn bị chuyển sang up_for_retry"""
    ti = context.get('task_instance')
    exception = context.get('exception')
    logger.warning(
        f"⚠️ [RETRY WARNING] Task '{ti.task_id}' gặp sự cố: {exception}. "
        f"Chuẩn bị retry lần thứ {ti.try_number}..."
    )

def on_pipeline_success_callback(context):
    """Kích hoạt khi TOÀN BỘ DAG Run thành công"""
    dag_id = context.get('dag').dag_id
    run_id = context.get('run_id')
    logger.info(f"✅ [SUCCESS] DAG Run '{run_id}' của DAG '{dag_id}' đã hoàn tất thành công!")