import pandas as pd

def calculate_daily_revenue(df: pd.DataFrame) -> float:
    """
    Tính tổng doanh thu từ dataframe giao dịch.
    Chỉ tính các giao dịch có trạng thái 'COMPLETED' và amount > 0.
    """
    if df.empty or "status" not in df.columns or "amount" not in df.columns:
        return 0.0
    
    valid_orders = df[(df["status"] == "COMPLETED") & (df["amount"] > 0)]
    return float(valid_orders["amount"].sum())