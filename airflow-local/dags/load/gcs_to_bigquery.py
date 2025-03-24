from airflow.decorators import task, dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from pendulum import datetime

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCS_MAFRA_AUCTION_BUCKET = Variable.get("GCS_MAFRA_AUCTION_BUCKET")
AUCTION_DATASET = Variable.get("AUCTION_DATASET")
AUCTION_TABLE = Variable.get("AUCTION_TABLE")


@dag(
    start_date=datetime(2025, 2, 18),
    schedule_interval="0 12 * * *",
    catchup=True,
    description="Append daily JSONL from GCS to BigQuery"
)
def gcs_to_bigquery_dag():
    @task
    def load_gcs_to_bq(**kwargs):
        GCSToBigQueryOperator(
            task_id="gcs_to_bigquery",
            gcp_conn_id="gcp-sample",
            bucket=GCS_MAFRA_AUCTION_BUCKET,
            source_objects=[f"mafra/auction/{kwargs['ds_nodash']}.jsonl"],
            schema_object="mafra_auction_schema.json",
            destination_project_dataset_table=f"{GCP_PROJECT_ID}:{AUCTION_DATASET}.{AUCTION_TABLE}",
            write_disposition="WRITE_APPEND",
            source_format="NEWLINE_DELIMITED_JSON",
            autodetect=True,
        ).execute(context=kwargs)

    load_gcs_to_bq()


gcs_to_bigquery_dag()
