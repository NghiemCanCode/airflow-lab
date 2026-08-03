import os
import logging


def process_single_partition_file(file_path: str, exchange_rate: float) -> dict:
    """
    Helper function đại diện cho công việc xử lý 1 partition dữ liệu.
    Hàm này thuần túy Python (Pure Function), độc lập hoàn toàn với Airflow Context.
    """
    file_name = os.path.basename(file_path)
    logging.info(f"Processing partition file: {file_name}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File không tồn tại: {file_path}")

    valid_count = 0
    error_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        # Bỏ qua dòng header
        for line in lines[1:]:
            parts = line.strip().split(',')
            if len(parts) < 4:
                error_count += 1
                continue

            try:
                amount = float(parts[2])
                if amount <= 0:
                    error_count += 1
                else:
                    valid_count += 1
            except ValueError:
                error_count += 1

    return {
        "file_name": file_name,
        "valid_count": valid_count,
        "error_count": error_count,
        "status": "SUCCESS"
    }