from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime

from custom_operators.data_go_abc import PublicDataToGCSOperator
from include.custom_operators.mafra.mafra_kat_sale_operator import MafraKatSaleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from json import JSONDecodeError
from airflow.exceptions import AirflowBadRequest
import json
from datetime import timedelta
from requests import Response


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    render_template_as_native_obj=True,
    catchup=False,
)
def kat_sale_to_bigquery():
    def validate_api_response(response) -> bool:
        try:
            json_data = response.json()
        except JSONDecodeError:
            raise AirflowBadRequest("API Response is not valid JSON")

        header = json_data["response"]["header"]
        if header["resultCode"] != "0":
            raise AirflowBadRequest(f"Error {header["resultCode"]}: {header["resultMsg"]}")
        total_count = json_data["response"]["body"]["totalCount"]
        print(f"Total Count : {json_data["response"]}")
        return False if total_count == 0 else True

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
        response_check=lambda response: validate_api_response(response),
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    def safe_response_filter(responses: Response | list[Response]):
        jsonl_str = ""
        for res in responses:
            if not res.text.strip():
                raise ValueError("API returned an empty response")
            content = res.json()
            total_count = content["response"]["body"]["totalCount"]
            item_list = content["response"]["body"]["items"]["item"]
            jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in item_list])
            if total_count == 0:
                return None
        return jsonl_str

    codes = ["210001", "210009", "380201", "370101", "320201", "320101", "320301", "110001", "110008",
             "310101", "310401", "310901", "311201", "230001", "230003", "360301", "240001", "240004", "350402",
             "350301", "350101", "250001", "250003", "330101", "340101", "330201", "370401", "371501", "220001",
             "380401", "380101", "380303"]

    # codes = ["110001"]

    # """
    # """
    # count_whsl = HttpOperator.partial(
    #     task_id="count_whsl",
    #     http_conn_id="datago_connection",
    #     endpoint="B552845/katSale/trades",
    #     method="GET",
    #     response_filter=safe_response_filter,
    # ).expand(
    #     data=[{
    #         "serviceKey": "{{ conn.datago_connection.extra_dejson.api_key }}",
    #         "pageNo": "1",
    #         "numOfRows": "1",
    #         "cond[trd_clcln_ymd::EQ]": "{{ macros.datetime.strptime(yesterday_ds, '%Y-%m-%d') + macros.timedelta(hours=9) }}",
    #         "cond[whsl_mrkt_cd::EQ]": str(code)
    #     } for code in codes])
    #
    # @task
    # def filter_valid_markets(count_whsl):
    #     return [data for data in count_whsl if data is not None]
    #
    # filtered_counts = filter_valid_markets(count_whsl.output)
    #
    # @task
    # def generate_page_info(count_data: dict):
    #     whsl_code, total_count = count_data.popitem()
    #     num_pages = (total_count + 999) // 1000
    #     return [{"whsl_mrkt_cd": whsl_code, "pageNo": page} for page in range(1, num_pages + 1)]
    #
    # page_info = generate_page_info.expand(count_data=filtered_counts)
    #
    # @task
    # def flatten_page_info(page_info_xcom):
    #     return [item for sublist in page_info_xcom for item in sublist]
    #
    # flattened_page = flatten_page_info(page_info)
    # flattened_page_info = [item for sublist in page_info for item in sublist]

    # kat_sale_to_gcs = MafraKatSaleToGCSOperator.partial(
    #     task_id="kat_sale_to_gcs",
    #     bucket_name="{{ var.value.gcs_raw_bucket }}",
    #     numOfRows="1000",
    #     pageNo=1,
    #     retries=5,
    #     retry_delay=timedelta(minutes=1),
    # ).expand(whsl_mrkt_cd=codes)

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

    kat_sale_to_gcs = PublicDataToGCSOperator.partial(
        task_id="kat_sale_to_gcs",
        bucket_name="{{ var.value.gcs_raw_bucket }}",
        object_name="mafra/kat_sale/{{ ds_nodash }}/",
        endpoint="B552845/katSale/trades",
        data={
            "pageNo": 1,
            "numOfRows": 1000,
            "cond[trd_clcln_ymd::EQ]": "{{ yesterday_ds }}",
            # "cond[whsl_mrkt_cd::EQ]": "110001",
        },
        retries=2,
        retry_delay=timedelta(minutes=1),
        response_filter=safe_response_filter,
        pagination_function=paginate,
        api_type=("query", "serviceKey")
    ).expand(
        expanded_data=[{"cond[whsl_mrkt_cd::EQ]": code} for code in codes]
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        trigger_rule='none_failed',
        gcp_conn_id="gcp-sample",
        bucket="{{ var.value.gcs_raw_bucket }}",
        source_objects=["mafra/kat_sale/*.jsonl"],
        destination_project_dataset_table="{{ var.value.GCP_PROJECT_ID }}:"
                                          "mafra."
                                          "kat_sale",
        schema_object="schemas/mafra__kat_sale_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    health_check_kat_sale >> kat_sale_to_gcs >> load_gcs_to_bq


kat_sale_to_bigquery()
