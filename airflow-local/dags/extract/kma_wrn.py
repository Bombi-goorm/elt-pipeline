from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma.kma_wrn_api_operator import KmaWrnApiOperator

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
        file_path="{{ ds_nodash }}.jsonl"
    )

    extract_kma_wrn_data

extract_kma_wrn()
