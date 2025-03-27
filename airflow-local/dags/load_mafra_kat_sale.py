from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import timedelta
from pendulum import datetime

from include.custom_operators.data_go_abc import PublicDataToGCSOperator
from helpers.common_utils import (datago_validate_api_response,
                                  datago_safe_response_filter,
                                  datago_paginate)

sale_dataset = Dataset("bigquery://bomnet.sale")


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    render_template_as_native_obj=True,
    catchup=False,
)
def load_mafra_kat_sale():
    health_check_kat_sale = HttpSensor(
        task_id="health_check_kat_sale",
        http_conn_id="datago_connection",
        endpoint="B552845/katSale/trades",
        method="GET",
        request_params={
            "serviceKey": "{{ conn.datago_connection.extra_dejson.api_key }}",
            "pageNo": "1",
            "numOfRows": "1",
            "cond[trd_clcln_ymd::EQ]": "{{ yesterday_ds }}",
            "cond[whsl_mrkt_cd::EQ]": "110001"
        },
        response_check=lambda response: datago_validate_api_response(response),
        poke_interval=30,
        timeout=600,
        mode="poke",
    )
    # TODO: .expand require list but @task's return value is xcom args when dag is parsed
    # @task(task_id="get_wholesale_market_codes")
    # def get_wholesale_market_codes() -> list[str]:
    #     from airflow.models import Variable
    #     return Variable.get("wholesale_market_codes")

    codes = ["210001", "210009", "380201", "370101", "320201", "320101", "320301", "110001", "110008",
             "310101", "310401", "310901", "311201", "230001", "230003", "360301", "240001", "240004", "350402",
             "350301", "350101", "250001", "250003", "330101", "340101", "330201", "370401", "371501", "220001",
             "380401", "380101", "380303"]

    kat_sale_to_gcs = PublicDataToGCSOperator.partial(
        task_id="kat_sale_to_gcs",
        bucket_name="{{ var.value.gcs_raw_bucket }}",
        object_name="mafra/kat_sale/{{ yesterday_ds }}/",
        endpoint="B552845/katSale/trades",
        data={
            "pageNo": 1,
            "numOfRows": 1000,
            "cond[trd_clcln_ymd::EQ]": "{{ yesterday_ds }}",
        },
        retries=2,
        retry_delay=timedelta(minutes=1),
        response_filter=datago_safe_response_filter,
        pagination_function=datago_paginate,
        api_type=("query", "serviceKey")
    ).expand(
        expanded_data=[{"cond[whsl_mrkt_cd::EQ]": code} for code in codes]
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        trigger_rule='none_failed',
        gcp_conn_id="google_cloud_bomnet_conn",
        bucket="{{ var.value.gcs_raw_bucket }}",
        source_objects=["mafra/kat_sale/{{ yesterday_ds }}/*.jsonl"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}:"
                                          "{{ val.value.mafra_dataset }}."
                                          "{{ val.value.kat_sale_table }}",
        schema_object="schemas/mafra__kat_sale_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        outlets=[sale_dataset]
    )

    health_check_kat_sale >> kat_sale_to_gcs >> load_gcs_to_bq


load_mafra_kat_sale()
