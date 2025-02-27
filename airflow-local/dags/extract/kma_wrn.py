from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma.kma_wrn_api_operator import KmaWrnApiOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


GCS_KMA_WRN_BUCKET = Variable.get("GCS_KMA_WRN_BUCKET")


@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_wrn():
    extract_kma_wrn_data = KmaWrnApiOperator(
        task_id="extract_kma_wrn_data",
        page_no=1,
        num_of_rows=1000,
        bucket_name=GCS_KMA_WRN_BUCKET,
    )
    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = "kma"
    WRN_TABLE = "wrn"
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket=GCS_KMA_WRN_BUCKET,
        source_objects=["{{  ds_nodash  }}.jsonl"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{WRN_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    extract_kma_wrn_data

extract_kma_wrn()
