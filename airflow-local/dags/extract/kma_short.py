import json

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma_short_api_operator import KmaShortApiOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator




@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_short():
    xy_combinations = [
        (38, 53),
        (74, 97),
        (75, 57),
        (75, 59),
        (89, 63),
        (91, 90),
        (101, 67),
        (107, 68),
        (114, 62),
        (115, 65),
        (119, 57),
        (119, 61),
        (120, 57),
        (120, 60),
        (120, 64),
        (120, 69)
    ]
    extract_kma_short_data = KmaShortApiOperator.partial(
        task_id="extract_kma_short_data",
        page_no=1,
        num_of_rows=290,
        base_time="0200",
        retries=2,
    ).expand(
        xy_pair=xy_combinations,
    )
    GCS_KMA_SHORT_BUCKET = Variable.get("GCS_KMA_SHORT_BUCKET")
    @task
    def upload_to_gcs(processed_data, **kwargs):
        if not processed_data:
            raise ValueError("No data found in XCom to upload to GCS.")
        items = processed_data['item']
        nx, ny = items[0]['nx'], items[0]['ny']
        jsonl_data = "\n".join([json.dumps(item, ensure_ascii=False) for item in items])

        gcs_hook = GCSHook(gcp_conn_id="gcp-sample")
        gcs_hook.upload(
            bucket_name=GCS_KMA_SHORT_BUCKET,
            object_name=f"{kwargs['ds_nodash']}/{nx}_{ny}.jsonl",
            data=jsonl_data,
            mime_type="application/json",
        )

    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = Variable.get("BQ_KMA_DATASET")
    SHORT_TABLE = Variable.get("BQ_KMA_SHORT_TABLE")

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket=GCS_KMA_SHORT_BUCKET,
        source_objects=["{{  ds_nodash  }}/*.jsonl"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{SHORT_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    upload_to_gcs.expand(processed_data=extract_kma_short_data.output) >> load_gcs_to_bq


extract_kma_short()
