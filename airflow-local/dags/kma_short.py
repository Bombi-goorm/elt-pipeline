from airflow.decorators import dag, task
from pendulum import datetime
from include.custom_operators.data_go_abc import PublicDataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.datasets import Dataset
from helpers.common_utils import datago_safe_response_filter


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_short():
    @task
    def get_region_coords_variable() -> list[dict[str, int]]:
        from airflow.models import Variable
        return Variable.get("region_coords", deserialize_json=True)

    region_coords = get_region_coords_variable()
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
        response_filter=datago_safe_response_filter,
    ).expand(expanded_data=region_coords)

    short_dataset = Dataset("bigquery://bomnet.short")
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["kma/short/{{ ds_nodash }}/*.jsonl"],
        schema_object="schemas/kma_short_schema.json",
        destination_project_dataset_table="{{ val.value.GCP_PROJECT_ID }}:{{ val.value.KMA_DATASET }}.{{ val.value.SHORT_TABLE }}",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        outlets=[short_dataset]
    )

    extract_kma_short_data >> load_gcs_to_bq


extract_kma_short()
