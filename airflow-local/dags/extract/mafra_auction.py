from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from include.custom_operators.mafra_api_operator import MafraAuctionToGCSOperator


whole_sale_codes = [
    "210001", "210009", "380201", "370101", "320201", "320101", "320301",
    # "210005", "110001", "110008", "310101", "310401", "310901", "311201",
    # "230001", "230003", "360301", "240001", "240004", "350402", "350301",
    # "350101", "250001", "250003", "330101", "340101", "330201", "370401",
    # "371501", "220001", "380401", "380101", "380303"
]

@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
)
def extract_mafra_auction():
    extract_task = MafraAuctionToGCSOperator.partial(
        task_id="extract_from_source",
        start_index=1,
        end_index=1000,
        bucket_name = "bomnet-raw"
    ).expand(whsal_cd=whole_sale_codes)

    extract_task

extract_mafra_auction()
