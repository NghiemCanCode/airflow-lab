import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    # Load toàn bộ DAG trong thư mục dags/ mà không cần chạy scheduler
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_import_errors(dagbag):
    """Đảm bảo KHÔNG CÓ DAG nào bị lỗi Syntax hoặc Import Error."""
    import_errors = dagbag.import_errors
    assert len(import_errors) == 0, f"DAG import errors found: {import_errors}"

def test_dag_has_tags_and_retries(dagbag):
    """Policy kiểm duyệt: Mọi DAG production đều phải gắn tag và có retry > 0."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' thiếu tags!"
        assert dag.default_args.get('retries', 0) > 0, f"DAG '{dag_id}' phải cấu hình retries > 0!"