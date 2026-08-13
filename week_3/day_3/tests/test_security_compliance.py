import os
import re
import pytest
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DAGS_DIR = os.path.join(BASE_DIR, "dags")

for p in [BASE_DIR, DAGS_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from airflow.models import DagBag

# Regex quét các chuỗi nghi ngờ là Secret hardcode
HARDCODED_SECRET_PATTERN = re.compile(
    r'(?i)(?:api_key|password|secret_key|access_token)\s*=\s*["\'](?!secret/|http|airflow)[A-Za-z0-9_\-]{12,}["\']'
)

def test_no_hardcoded_secrets_in_dags():
    """Đảm bảo không có credential bị hardcode trong thư mục dags/"""
    dagbag = DagBag(dag_folder="/opt/airflow/dags")
    assert len(dagbag.import_errors) == 0, f"DAG Import Errors: {dagbag.import_errors}"
    
    for root, _, files in os.walk("dags"):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    matches = HARDCODED_SECRET_PATTERN.findall(content)
                    assert not matches, f"Potential hardcoded secret found in {file_path}: {matches}"

def test_vault_backend_is_configured():
    """Đảm bảo môi trường bắt buộc phải bật AIRFLOW__SECRETS__BACKEND"""
    secrets_backend = os.getenv("AIRFLOW__SECRETS__BACKEND")
    assert secrets_backend is not None, "AIRFLOW__SECRETS__BACKEND must be configured in production!"
    assert "VaultBackend" in secrets_backend, "Backend must use HashiCorp Vault!"