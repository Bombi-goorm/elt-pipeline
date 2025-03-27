from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.datasets import Dataset
from include.custom_operators.data_go_abc import PublicDataToGCSOperator
from helpers.common_utils import datago_safe_response_filter, datago_paginate
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


@dag(
    schedule_interval='*/10 * * * *',
    start_date=datetime(2025, 2, 18),
    catchup=False,
)
def load_mafra_realtime():
    real_time_to_gcs = PublicDataToGCSOperator(
        task_id="real_time_to_gcs",
        bucket_name="bomnet-raw",
        data={
            "pageNo": 1,
            "numOfRows": 10000,
            "dataType": "json"
        },
        object_name="mafra/real_time/{{ ds_nodash }}.jsonl",
        endpoint="B552845/katRealTime/trades",
        response_filter=datago_safe_response_filter,
        pagination_function=datago_paginate,
        api_type=("query", "serviceKey"),
    )
    realtime_dataset = Dataset("bigquery://bomnet.realtime")
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="google_cloud_bomnet_conn",
        bucket="bomnet-raw",
        source_objects=["mafra/real_time/{{ ds_nodash }}.jsonl"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}:"
                                          "{{ val.value.mafra_dataset }}."
                                          "{{ val.value.kat_realtime_table }}",
        schema_object="schemas/kat_real_time_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        outlets=[realtime_dataset],
    )
    real_time_to_gcs >> load_gcs_to_bq


load_mafra_realtime()
