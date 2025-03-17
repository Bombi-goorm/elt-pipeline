from airflow.decorators import dag
from airflow.models import Variable
from requests import Response
from custom_operators.data_go_abc import PublicDataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import json


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def kma_wrn_to_bigquery():
    def response_filter(responses: Response | list[Response]) -> str:
        jsonl_str = ""
        for res in responses:
            print(type(res))
            print(res.text)
            content = res.json()
            item_list = content["response"]["body"]["items"]["item"]
            jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in item_list])
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
        object_name="kma/wrn/{{ ds_nodash }}.jsonl",
        endpoint="/1360000/WthrWrnInfoService/getWthrWrnList",
        response_filter=response_filter,
        pagination_function=paginate,
        api_type=("query", "serviceKey")
    )

    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = "kma"
    WRN_TABLE = "wrn"
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["kma/wrn/{{ ds_nodash }}.jsonl"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{WRN_TABLE}",
        schema_object="schemas/kma_wrn_schema.json",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    extract_kma_wrn_data >> load_gcs_to_bq


kma_wrn_to_bigquery()
