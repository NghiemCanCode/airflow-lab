import pandas as pd
import os

def read_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def write_data(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)