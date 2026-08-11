import logging
from include.exceptions import TransientError, FatalDataError

logger = logging.getLogger(__name__)

class ExternalDataService:
    @staticmethod
    def fetch_api_data(try_number: int):
        logger.info(f"Đang gọi External API (Attempt: {try_number})...")
        # Giả lập 2 lần đầu bị timeout, lần thứ 3 mới thành công
        if try_number < 3:
            raise TransientError(f"HTTP 503: Service Unavailable (Attempt {try_number})")
        return [{"id": 101, "amount": 500}, {"id": 102, "amount": -50}]

    @staticmethod
    def process_data(data: list, fail_mode: bool = False):
        if fail_mode:
            raise FatalDataError("FATAL: Phát hiện bản ghi không hợp lệ (amount < 0)!")
        return [item for item in data if item["amount"] > 0]