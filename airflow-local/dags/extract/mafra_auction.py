from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from include.custom_operators.mafra_api_operator import MafraApiOperator



@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=True,
)
def extract_mafra_auction_backfill():
    extract_task = MafraApiOperator.partial(
        task_id="extract_from_source",
        start_index=1,
        end_index=1000,
    ).expand(whsal_cd=["110001", "380401"])

    @task
    def upload_to_gcs(jsonl_data, **kwargs):
        GCS_MAFRA_AUCTION_BUCKET = Variable.get("GCS_MAFRA_AUCTION_BUCKET")
        object_name = "{{ ds_nodash }}.jsonl"
        if not jsonl_data:
            raise ValueError("No data found in XCom to upload to GCS.")
        jsonl_string = "\n".join(jsonl_data)
        gcs_hook = GCSHook(gcp_conn_id="gcp-sample")
        gcs_hook.upload(
            bucket_name=GCS_MAFRA_AUCTION_BUCKET,
            object_name=f"{kwargs['ds_nodash']}.jsonl",
            data=jsonl_string,
            mime_type="application/json",
        )

    upload_to_gcs(extract_task.output)


extract_mafra_auction_backfill()
