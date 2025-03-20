from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from airflow.decorators import dag
from airflow.datasets import DatasetAlias

dataset_alias = "kat_real_time_gcs"


@dag(
    schedule=[DatasetAlias(dataset_alias)],
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def gcs_to_bigquery():
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        gcp_conn_id="gcp-sample",
        bucket="bomnet-raw",
        source_objects=["mafra/real_time/{{ ds_nodash }}.jsonl"],
        destination_project_dataset_table=f"{"goorm-bomnet"}:{"mafra"}.{"real_time"}",
        schema_object="schemas/kat_real_time_schema.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
    )


gcs_to_bigquery()
