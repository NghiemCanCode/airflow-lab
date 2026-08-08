import argparse
import sys
from etl_engine.domain import transform_sales_data
from etl_engine.storage import read_data, write_data

def main():
    parser = argparse.ArgumentParser(description="ETL Container CLI Engine")
    parser.add_argument("--input-path", required=True, help="Path to raw input CSV")
    parser.add_argument("--output-path", required=True, help="Path to save processed CSV")
    parser.add_argument("--min-amount", type=float, default=0.0, help="Minimum amount filter")
    
    args = parser.parse_args()
    
    # FIX: Đổi args.input-path -> args.input_path (gạch dưới _)
    print(f"[CLI Engine] Starting process: input={args.input_path}, output={args.output_path}")
    
    # --- BẮT ĐẦU GỌI CODE SANG CÁC MODULE KHÁC ---
    # 1. Gọi storage.py
    df = read_data(args.input_path) 
    
    # 2. Gọi domain.py (Pure Business Logic)
    result_df = transform_sales_data(df, min_amount=args.min_amount) 
    
    # 3. Gọi storage.py
    write_data(result_df, args.output_path) 
    # ----------------------------------------------
    
    print(f"[CLI Engine] Success! Saved {len(result_df)} rows to {args.output_path}")

if __name__ == "__main__":
    main()  