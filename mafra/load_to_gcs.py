from google.cloud import storage
import json
from datetime import datetime


def process_json_data(json_data):
    rows = json_data["Grid_20240625000000000654_1"]["row"]
    filtered_rows = [
        {key: value for key, value in row.items() if key not in ["ROW_NUM", "SALEDATE"]}
        for row in rows
    ]
    jsonl_data = "\n".join([json.dumps(row, ensure_ascii=False) for row in filtered_rows])

    return jsonl_data


def upload_to_gcs(bucket_name, jsonl_data, service_account_json):
    """JSONL 데이터를 GCS에 업로드"""
    storage_client = storage.Client.from_service_account_json(service_account_json)
    bucket = storage_client.bucket(bucket_name)

    # 현재 시각 기반 파일명 생성
    current_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    file_name = f"{current_time}.jsonl"

    blob = bucket.blob(file_name)
    blob.upload_from_string(jsonl_data, content_type="application/jsonl")

    print(f"✅ 파일 업로드 완료: gs://{bucket_name}/{file_name}")


def load_data_to_gcs(json_data, bucket_name, service_account_json):
    """JSON 데이터를 처리하고 GCS에 저장"""
    jsonl_data = process_json_data(json_data)
    upload_to_gcs(bucket_name, jsonl_data, service_account_json)
