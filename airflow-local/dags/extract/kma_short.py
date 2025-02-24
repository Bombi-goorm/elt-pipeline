import json

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from include.custom_operators.kma_wrn_api_operator import KmaShortApiOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
AUCTION_BUCKET_NAME = Variable.get("AUCTION_BUCKET_NAME")


@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def extract_kma_short():
    extract_kma_short_data = KmaShortApiOperator.partial(
        task_id="extract_kma_wrn_data",
        page_no=1,
        num_of_rows=1000,
        nx=-1,
        ny=-1,
        base_time=-1,).expand()
    # x, y에 대한 요청을 보내야 함 동적으로



    @task
    def upload_to_gcs(processed_data, **kwargs):
        if not processed_data:
            raise ValueError("No data found in XCom to upload to GCS.")
        gcs_hook = GCSHook(gcp_conn_id="gcp_sample")
        gcs_hook.upload(
            bucket_name=AUCTION_BUCKET_NAME,
            object_name=f"{kwargs['ds_nodash']}.json",
            data=json.dumps(processed_data),
            mime_type="application/json",
        )

    upload_to_gcs(extract_kma_short_data.output)


extract_kma_short()
