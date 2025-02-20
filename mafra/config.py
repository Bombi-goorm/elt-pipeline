import os
import dotenv

dotenv.load_dotenv()

BASE_URL = "http://211.237.50.150:7080/openapi"
API_KEY = os.getenv("API_KEY")
SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH")

START_INDEX = 1
END_INDEX = 1000
API_URL = "Grid_20240625000000000654_1"
OUTPUT_PATH = "../outputs/output.jsonl"

DATASET_ID = "mafra"
TABLE_ID = "auction"

BUCKET_NAME = "bomnet-gcs"