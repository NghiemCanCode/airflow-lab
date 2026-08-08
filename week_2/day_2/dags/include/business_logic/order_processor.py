import csv
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_records(raw_data: list[dict]) -> tuple[list[dict], list[dict]]:
    """Phân loại dữ liệu hợp lệ và không hợp lệ."""
    valid_records = []
    invalid_records = []

    for row in raw_data:
        try:
            amount = float(row.get("amount", 0))
            customer_id = row.get("customer_id", "").strip()

            # Rule validate: Amount > 0 và Customer ID không rỗng
            if amount > 0 and customer_id:
                valid_records.append(row)
            else:
                invalid_records.append(
                    {**row, "reason": "Invalid amount or missing customer_id"}
                )
        except ValueError:
            invalid_records.append({**row, "reason": "Amount parsing error"})

    return valid_records, invalid_records


def transform_records(valid_data: list[dict], exchange_rate_eur: float = 1.1) -> list[dict]:
    """Chuyển đổi dữ liệu: Quy đổi tiền tệ về USD và chuẩn hóa thông tin."""
    transformed = []
    for row in valid_data:
        amount = float(row["amount"])
        currency = row["currency"].upper()

        if currency == "EUR":
            amount_usd = round(amount * exchange_rate_eur, 2)
        else:
            amount_usd = round(amount, 2)

        transformed.append(
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "amount_usd": amount_usd,
                "original_amount": amount,
                "original_currency": currency,
                "status": row["status"].lower(),
                "order_date": row["order_date"],
            }
        )
    return transformed

# ============== Comment the following function to Step 8 ====================

def export_to_json_atomically(data: list[dict], target_path: str, staging_dir: str) -> str:
    """Ghi dữ liệu ra file JSON theo chiến lược Atomic Write (Write to Temp -> Rename)."""
    Path(staging_dir).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(target_path)).mkdir(parents=True, exist_ok=True)

    file_name = os.path.basename(target_path)
    temp_path = os.path.join(staging_dir, f"tmp_{file_name}")

    # Bước 1: Ghi vào file tạm (.tmp)
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Bước 2: Atomic Replace (Ghi đè nguyên tử lên file đích)
    os.replace(temp_path, target_path)
    logger.info(f"Successfully atomic-wrote data to {target_path}")
    return target_path

# ============== Uncomment the following function to Step 8 ====================

    # def export_to_json_atomically(data: list[dict], target_path: str, staging_dir: str) -> str:
    #     Path(staging_dir).mkdir(parents=True, exist_ok=True)
    #     file_name = os.path.basename(target_path)
    #     temp_path = os.path.join(staging_dir, f"tmp_{file_name}")

    #     with open(temp_path, "w", encoding="utf-8") as f:
    #         f.write('{"partial_data": [') # Ghi dở dang
    #         raise RuntimeError("Simulated Power Outage / Disk Failure!") # Giả lập crash

    #     os.replace(temp_path, target_path)
    #     return target_path