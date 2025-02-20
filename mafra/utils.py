import json
from datetime import datetime


def write_to_jsonl(data, output_path, key):
    if key not in data:
        print("Error: Invalid key in API response.")
        return

    row_data = data[key]["row"]

    with open(output_path, 'w', encoding='utf-8') as f:
        for row in row_data:
            json.dump(row, f, ensure_ascii=False)
            f.write('\n')

    print(f"Data saved to {output_path}")


def process_json_data(json_data, exclude_keys=None):
    exclude_keys = exclude_keys or []
    rows = json_data["Grid_20240625000000000654_1"]["row"]

    filtered_rows = [{k: v for k, v in row.items() if k not in exclude_keys} for row in rows]
    jsonl_data = "\n".join([json.dumps(row, ensure_ascii=False) for row in filtered_rows])

    return jsonl_data


def generate_filename(prefix=""):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    return f"{prefix}{timestamp}.jsonl"
