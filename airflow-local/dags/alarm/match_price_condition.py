from airflow import Dataset
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from datetime import datetime
from airflow.decorators import dag

realtime_dataset = Dataset("bigquery://bomnet.realtime")

dbt_realtime = Dataset("bigquery://bomnet.transform.realtime")


@dag(
    schedule=[dbt_realtime],
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def match_price_condition():
    from_mysql_to_gcs = MySQLToGCSOperator(
        task_id="from_mysql_to_gcs",
        sql="SELECT * FROM notification_condition;",
        bucket="bomnet-raw",
        filename="aws_rds/price_conditions.csv",  # 저장할 파일명 (JSON)
        # schema_filename="mysql_schema.json",  # 스키마 파일 (선택 사항)
        export_format="csv",
        field_delimiter=",",
        mysql_conn_id="mysql_test_connection",
        gcp_conn_id="gcp-sample",
        outlets=[realtime_dataset]
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        gcp_conn_id="gcp-sample",
        source_objects=["aws_rds/price_conditions.csv"],
        bucket="bomnet-raw",
        destination_project_dataset_table="goorm-bomnet:aws_rds.price_conditions",
        # schema_object="schemas/kat_real_time_schema.json",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        autodetect=True,
    )

    from_mysql_to_gcs >> gcs_to_bq


match_price_condition()
