from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma.kma_wrn_api_operator import KmaWrnToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator




@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_wrn():
    extract_kma_wrn_data = KmaWrnToGCSOperator(
        task_id="extract_kma_wrn_data",
        page_no=1,
        num_of_rows=1000,
        bucket_name="bomnet-raw",
    )
    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    KMA_DATASET = "kma"
    WRN_TABLE = "wrn"
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["kma/wrn/{{  ds_nodash  }}.jsonl"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}:{KMA_DATASET}.{WRN_TABLE}",
        schema_object="schemas/kma_wrn_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )

    extract_kma_wrn_data >> load_gcs_to_bq

extract_kma_wrn()
