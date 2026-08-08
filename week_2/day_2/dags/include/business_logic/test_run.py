from order_processor import validate_records, transform_records, export_to_json_atomically
import csv

if __name__ == "__main__":
    with open("../data/input/raw_orders.csv", "r") as f:
        data = list(csv.DictReader(f))
    
    valid, invalid = validate_records(data)
    transformed = transform_records(valid)
    export_to_json_atomically(transformed, "../data/output/test_out.json", "../data/staging")
    print(f"Test OK! Valid records: {len(valid)}, Invalid: {len(invalid)}")