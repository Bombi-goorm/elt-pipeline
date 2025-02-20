from api_client import APIClient
import config
# from load_to_gcs import load_data_to_gcs
from gcs_client import GCSClient
from utils import process_json_data


def main():
    params = {
        "SALEDATE": "20250217",
        "WHSALCD": "110001",
        "CMPCD": "11000104",
    }

    api_client = APIClient(config.BASE_URL, config.API_KEY, config.API_URL, config.START_INDEX, config.END_INDEX, params)
    response_data = api_client.send_request()
    jsonl_data = process_json_data(response_data, exclude_keys=["ROW_NUM", "SALEDATE"])

    if response_data:
        gcs_client = GCSClient(config.BUCKET_NAME, config.SERVICE_ACCOUNT_PATH)
        # gcs_client.set_bucket_lifecycle({"action": {"type": "Delete"}, "condition": {"age": 1}})
        gcs_client.upload_jsonl(jsonl_data, prefix="auction_")
        # load_data_to_bigquery(config.OUTPUT_PATH, config.DATASET_ID, config.TABLE_ID, config.SERVICE_ACCOUNT_PATH)
    else:
        print("API 요청 실패")


if __name__ == "__main__":
    main()
