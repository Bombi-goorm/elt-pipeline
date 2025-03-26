from airflow import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from include.custom_operators.data_go_abc import PublicDataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from helpers.common_utils import datago_safe_response_filter, datago_paginate

wrn_dataset = Dataset("bigquery://bomnet.wrn")


@dag(
    schedule_interval=timedelta(hours=2),
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def load_kma_wrn():
    kma_wrn_to_gcs = PublicDataToGCSOperator(
        task_id="extract_kma_wrn",
        bucket_name="bomnet-raw",
        data={
            "pageNo": 1,
            "numOfRows": 100,
            "dataType": "json"
        },
        object_name="kma/wrn/{{ ds_nodash }}.jsonl",
        endpoint="/1360000/WthrWrnInfoService/getWthrWrnList",
        response_filter=datago_safe_response_filter,
        pagination_function=datago_paginate,
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
        outlets=[wrn_dataset],
    )

    kma_wrn_to_gcs >> load_gcs_to_bq


load_kma_wrn()
