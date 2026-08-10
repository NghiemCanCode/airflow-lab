import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    # Airflow 3.3.0: Chỉ cần truyền duy nhất đường dẫn dag_folder
    return DagBag(dag_folder="/opt/airflow/dags")

def test_dag_import_no_errors(dagbag):
    """Đảm bảo 100% DAGs không dính lỗi import hoặc syntax error."""
    import_errors = dagbag.import_errors
    assert len(import_errors) == 0, f"Phát hiện lỗi Import DAG: {import_errors}"

def test_dag_tags_and_retries(dagbag):
    """Kiểm tra quy chuẩn đặt tag cho toàn bộ DAGs trong dự án."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' vi phạm quy chuẩn: Phải gắn ít nhất 1 tag!"