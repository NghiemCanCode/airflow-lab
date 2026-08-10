import pytest
import logging
from include.etl_processor import DataETLProcessor

def test_process_records_success_and_logging(caplog):
    processor = DataETLProcessor(min_score_threshold=0, bonus_multiplier=1.5)
    mock_data = [
        {"user_id": "U1", "score": 10},
        {"user_id": "U2", "score": -5}
    ]

    # Bắt log ở mức INFO trở lên
    with caplog.at_level(logging.INFO):
        result = processor.process_records(mock_data)

    # 1. Kiểm tra logic dữ liệu
    assert len(result) == 1
    assert result[0]["user_id"] == "U1"
    assert result[0]["final_score"] == 15.0

    # 2. Kiểm tra câu lệnh Log sinh ra
    assert "Bắt đầu tiến trình ETL. Tổng số bản ghi nhận vào: 2" in caplog.text
    assert "bị BỎ QUA do score (-5) nhỏ hơn ngưỡng cho phép" in caplog.text
    assert "Số bản ghi xử lý thành công: 1/2" in caplog.text

def test_process_records_warning_level_log(caplog):
    processor = DataETLProcessor(min_score_threshold=10, bonus_multiplier=1.0)
    mock_data = [{"user_id": "U_WARN", "score": 5}]

    with caplog.at_level(logging.WARNING):
        processor.process_records(mock_data)

    # Kiểm tra log WARNING được bắn ra đúng khi score < threshold
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert "User 'U_WARN' bị BỎ QUA" in caplog.text