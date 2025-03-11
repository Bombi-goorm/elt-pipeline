from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable, TaskInstance
from requests import Response

from custom_operators.data_go_abc import PublicDataToGCSOperator
# from include.custom_operators.kma.kma_wrn_api_operator import KmaWrnToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import json


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_wrn():
    def response_filter(responses: Response | list[Response], ti: TaskInstance) -> str:
        updated_at = ti.xcom_pull(key="updated_at", default="2025.01.01.00:00")
        timestamp_updated_at = datetime.strptime(updated_at, "%Y.%m.%d.%H:%M")
        jsonl_str = ""
        latest_timestamp = updated_at

        for res in responses:
            content = res.json()
            if not content["response"].get("body"):
                break
            item_list = content["response"]["body"]["items"]["item"]
            for wrn_item in item_list:
                ts = wrn_item["tmFc"]
                timestamp_ts = datetime.strptime(ts, "%Y.%m.%d.%H:%M")
                latest_timestamp = max(latest_timestamp, timestamp_ts)
                if timestamp_ts <= timestamp_updated_at:
                    break
                jsonl_str += json.dumps(wrn_item, ensure_ascii=False) + "\n"
        ti.xcom_push(key="updated_at", value=latest_timestamp)
        return jsonl_str

    def paginate(response: Response) -> dict | None:
        content = response.json()
        if not content["response"].get("body"):
            return None
        body = content["response"]["body"]
        total_count = body["totalCount"]
        cur_page_no = body["pageNo"]
        cur_num_of_rows = body["numOfRows"]
        if cur_page_no * cur_num_of_rows < total_count:
            return dict(params={"pageNo": cur_page_no + 1, })

    extract_kma_wrn_data = PublicDataToGCSOperator(
        task_id="extract_kma_wrn",
        bucket_name="bomnet-raw",
        data={
            "pageNo": 1,
            "numOfRows": 100,
            "dataType": "json"
        },
        object_name=f"kma/wrn/{{ ds_nodash }}.jsonl",
        endpoint="/1360000/WthrWrnInfoService/getWthrWrnList",
        # response_check=lambda response: response.json()["body"]["resultCode"] == "00",
        response_filter=response_filter,
        pagination_function=paginate,
        api_type=("query", "serviceKey")
    )

    # extract_kma_wrn_data = KmaWrnToGCSOperator(
    #     task_id="extract_kma_wrn_data",
    #     page_no=1,
    #     num_of_rows=10,
    #     bucket_name="bomnet-raw",
    # )

    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = "kma"
    WRN_TABLE = "wrn"
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["kma/wrn/{{  ds_nodash  }}.jsonl"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{WRN_TABLE}",
        schema_object="schemas/kma_wrn_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    extract_kma_wrn_data >> load_gcs_to_bq


extract_kma_wrn()
