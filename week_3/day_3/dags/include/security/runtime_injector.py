import logging
from typing import Dict, Any
from airflow.models.connection import Connection
from airflow.sdk.bases.hook import BaseHook
import copy

logger = logging.getLogger(__name__)


def get_ephemeral_db_connection(conn_id: str, dynamic_schema: str = None):
    """
    Fetch connection details from Secret Backend and dynamically inject runtime properties
    without persisting anything to DB/Disk.
    """
    logger.info("Resolving connection '%s' via Secrets Backend Chain...", conn_id)
    
    # BaseHook tự động tìm theo thứ tự: Env -> Vault -> Metadata DB
    base_conn = BaseHook.get_connection(conn_id)
    
    if dynamic_schema:
        logger.info("Injecting dynamic schema '%s' into connection at runtime.", dynamic_schema)
        # Tạo bản sao Connection trong RAM và override schema
        injected_conn = copy.copy(base_conn)
        injected_conn.schema = dynamic_schema
        return injected_conn

    return base_conn