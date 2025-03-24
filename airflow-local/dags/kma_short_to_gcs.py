from datetime import datetime
from airflow.decorators import dag
from airflow.datasets import DatasetAlias
from plugins.custom_operators.data_go_abc import PublicDataToGCSOperator
from helpers.common_utils import datago_safe_response_filter, datago_paginate

realtime_alias = "kat_real_time_gcs"


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    catchup=False,
)
def kat_realtime_to_gcs():
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
        outlets=[DatasetAlias(realtime_alias)],
        alias_name=realtime_alias,
    )
    real_time_to_gcs


kat_realtime_to_gcs()
