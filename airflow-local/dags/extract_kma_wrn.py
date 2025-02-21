from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma_wrn_api_operator import KmaWrnApiOperator

AUCTION_BUCKET_NAME = Variable.get("AUCTION_BUCKET_NAME")


@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_wrn():
    extract_task = KmaWrnApiOperator.partial(
        task_id="extract_from_source",
        start_index=1,
        end_index=1000,
    ).expand(whsal_cd=["110001", "380401"])

    @task
    def upload_to_gcs(jsonl_data, **kwargs):
        object_name = "{{ ds_nodash }}.jsonl"
        if not jsonl_data:
            raise ValueError("No data found in XCom to upload to GCS.")
        jsonl_string = "\n".join(jsonl_data)
        gcs_hook = GCSHook(gcp_conn_id="gcp_sample")
        gcs_hook.upload(
            bucket_name=AUCTION_BUCKET_NAME,
            object_name=f"{kwargs['ds_nodash']}.jsonl",
            data=jsonl_string,
            mime_type="application/json",
        )

    extract_data = extract_task.output
    upload_to_gcs(extract_data)

extract_kma_wrn()
