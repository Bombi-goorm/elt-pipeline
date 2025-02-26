from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma.kma_short_api_operator import KmaShortApiOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_KMA_SHORT_BUCKET = Variable.get("GCS_KMA_SHORT_BUCKET")


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
        bucket_name=GCS_KMA_SHORT_BUCKET,
        file_path="{{ ds_nodash }}/{{ params.xy_pair }}.jsonl"
        # retries=2,
    ).expand(
        xy_pair=xy_combinations,
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

    extract_kma_short_data >> load_gcs_to_bq


extract_kma_short()
