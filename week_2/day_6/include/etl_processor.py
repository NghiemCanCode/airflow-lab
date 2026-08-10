import logging

logger = logging.getLogger(__name__)

class DataETLProcessor:
    def __init__(self, min_score_threshold: float, bonus_multiplier: float):
        self.min_score_threshold = min_score_threshold
        self.bonus_multiplier = bonus_multiplier

    def process_records(self, records: list[dict]) -> list[dict]:
        logger.info("Bắt đầu tiến trình ETL. Tổng số bản ghi nhận vào: %d", len(records))
        processed = []
        
        for idx, item in enumerate(records):
            user_id = item.get("user_id", "UNKNOWN")
            score = item.get("score", 0)

            if score < self.min_score_threshold:
                logger.warning(
                    "Bản ghi [%d] User '%s' bị BỎ QUA do score (%s) nhỏ hơn ngưỡng cho phép (%s)", 
                    idx, user_id, score, self.min_score_threshold
                )
                continue

            calculated_score = score * self.bonus_multiplier
            logger.debug("User '%s': score gốc = %s -> score sau nhân hệ số = %s", user_id, score, calculated_score)
            
            processed.append({
                "user_id": user_id,
                "final_score": calculated_score,
                "status": "PASSED"
            })

        logger.info("Kết thúc ETL. Số bản ghi xử lý thành công: %d/%d", len(processed), len(records))
        return processed