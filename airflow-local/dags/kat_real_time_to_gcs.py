from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from requests import Response
import json
from typing import Optional, Dict, List, Union
from airflow.datasets import Dataset, DatasetAlias

from custom_operators.data_go_abc import PublicDataToGCSOperator

real_time_alias = "kat_real_time_gcs"


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    catchup=False,
)
def kat_real_time_to_gcs():
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

    def safe_response_filter(responses: Union[Response, List[Response]]) -> Optional[str]:
        jsonl_list = []
        for res in responses if isinstance(responses, list) else [responses]:
            if not res.text.strip():
                raise ValueError("API returned an empty response")
            content = res.json()
            total_count = content["response"]["body"]["totalCount"]
            if total_count == 0:
                return None
            item_list = content["response"]["body"]["items"]["item"]
            jsonl_list.extend(json.dumps(item, ensure_ascii=False) for item in item_list)
        return "\n".join(jsonl_list) if jsonl_list else None

    price_to_gcs = PublicDataToGCSOperator(
        task_id="price_to_gcs",
        bucket_name="bomnet-raw",
        data={
            "pageNo": 1,
            "numOfRows": 10000,
            "dataType": "json"
        },
        object_name="mafra/real_time/{{ ds_nodash }}.jsonl",
        endpoint="B552845/katRealTime/trades",
        response_filter=safe_response_filter,
        pagination_function=paginate,
        api_type=("query", "serviceKey"),
        outlets=[DatasetAlias(real_time_alias)],
        alias_name=real_time_alias,
    )
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["mafra/real_time/{{ ds_nodash }}.jsonl"],
        destination_project_dataset_table=f"{"goorm-bomnet"}:{"mafra"}.{"real_time"}",
        schema_object="schemas/kat_real_time_schema.json",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )
    price_to_gcs >> load_gcs_to_bq


kat_real_time_to_gcs()
