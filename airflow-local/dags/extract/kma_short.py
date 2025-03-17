from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime
from airflow.models import Variable
from requests import Response
from custom_operators.data_go_abc import PublicDataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import json


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_short():
    xy_combinations = [
        (43, 123),
        (53, 38),
        (54, 125),
        (55, 128),
        (56, 125),
        (56, 134),
        (57, 119),
        (57, 120),
        (57, 75),
        (58, 125),
        (58, 128),
        (59, 122),
        (59, 75),
        (60, 120),
        (60, 122),
        (60, 123),
        (60, 124),
        (60, 127),
        (60, 132),
        (61, 119),
        (61, 130),
        (61, 134),
        (61, 138),
        (62, 114),
        (62, 124),
        (63, 127),
        (63, 128),
        (63, 133),
        (63, 89),
        (64, 120),
        (64, 126),
        (65, 115),
        (65, 124),
        (67, 101),
        (68, 107),
        (69, 120),
        (69, 125),
        (71, 120),
        (90, 91),
        (93, 132),
        (97, 74)
    ]
    xy_combinations = [{"nx": x, "ny": y} for x, y in xy_combinations]

    def response_filter(responses: Response | list[Response]) -> str:
        jsonl_str = ""
        if type(responses) == list:
            for res in responses:
                print(type(res))
                print(res.text)

                content = res.json()
                item_list = content["response"]["body"]["items"]["item"]
                jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in item_list])
        else:
            print(responses.text)

            content = responses.json()
            item_list = content["response"]["body"]["items"]["item"]
            jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in item_list])
        return jsonl_str

    extract_kma_short_data = PublicDataToGCSOperator.partial(
        task_id="extract_kma_short_data",
        bucket_name="bomnet-raw",
        object_name="kma/short/{{ ds_nodash }}/",
        endpoint="/1360000/VilageFcstInfoService_2.0/getVilageFcst",
        data={
            "pageNo": 1,
            "numOfRows": 290,
            "base_time": "0200",
            "base_date": "{{ ds_nodash }}",
            "dataType": "json"
        },
        api_type=("query", "serviceKey"),
        response_filter=response_filter,
    ).expand(expanded_data=xy_combinations)

    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = Variable.get("BQ_KMA_DATASET")
    SHORT_TABLE = Variable.get("BQ_KMA_SHORT_TABLE")

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["kma/short/{{ ds_nodash }}/*.jsonl"],
        schema_object="schemas/kma_short_schema.json",
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{SHORT_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    extract_kma_short_data >> load_gcs_to_bq


extract_kma_short()
