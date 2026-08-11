class TransientError(Exception):
    """Lỗi tạm thời (Mạng gián đoạn, Timeout, Service bận) -> CẦN RETRY"""
    pass

class FatalDataError(Exception):
    """Lỗi logic/dữ liệu hỏng (Sai schema, Giá trị âm) -> KHÔNG RETRY, NGẮT MẠCH NGAY"""
    pass