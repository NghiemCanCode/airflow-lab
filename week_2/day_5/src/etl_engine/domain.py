import pandas as pd
from typing import Dict, Any

def transform_sales_data(df: pd.DataFrame, min_amount: float) -> pd.DataFrame:
    """Pure business logic: Lọc dữ liệu và tính toán chỉ số."""
    if df.empty:
        raise ValueError("Input DataFrame is empty")
        
    filtered_df = df[df['amount'] >= min_amount].copy()
    filtered_df['amount_usd'] = filtered_df['amount'] / 25000.0
    filtered_df['processed_at'] = pd.Timestamp.now()
    return filtered_df