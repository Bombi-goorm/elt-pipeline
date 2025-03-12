from datetime import datetime
from airflow.decorators import dag, task

from custom_operators.data_go_abc import PublicDataToGCSOperator


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    catchup=False,

)
def kat_real_time_to_gcs():
    price_to_gcs = PublicDataToGCSOperator(
        task_id="price_to_gcs",

    )
