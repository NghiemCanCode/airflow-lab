import os
import sys
import argparse
import polars as pl

def run_etl(execution_date: str, input_path: str, output_path: str):
    print(f"[ETL Container] Starting processing for logical date: {execution_date}")
    
    if not os.path.exists(input_path):
        print(f"[ETL Container] ERROR: Input path {input_path} does not exist!", file=sys.stderr)
        sys.exit(1) # Trả về Exit code khác 0 để báo lỗi

    # Đọc dữ liệu mẫu bằng Polars (thư viện không có trên Airflow worker)
    df = pl.read_csv(input_path)
    
    # Transform: Thêm cột metadata và lọc dữ liệu
    df_transformed = df.with_columns([
        pl.lit(execution_date).alias("processed_at"),
        (pl.col("amount") * 1.1).alias("amount_with_tax")
    ])
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_transformed.write_csv(output_path)
    
    print(f"[ETL Container] Processed {len(df_transformed)} rows successfully.")
    print(f"[ETL Container] Output saved to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    args = parser.parse_args()

    run_etl(args.date, args.input, args.output)