from airflow.decorators import dag
from pendulum import datetime
from requests import Response
from airflow.models import TaskInstance
import json
# from ..helpers.common_utils import paginate
from datetime import datetime
from custom_operators.data_go_abc import PublicDataToGCSOperator


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
)
def extract_mafra_kat_real_time():
    def response_filter(responses: Response | list[Response], ti: TaskInstance) -> str:
        updated_at = ti.xcom_pull(key="updated_at", default="2025.01.01.00:00")
        timestamp_updated_at = datetime.strptime(updated_at, "%Y.%m.%d.%H:%M")
        jsonl_str = ""
        latest_timestamp = updated_at

        for res in responses:
            content = res.json()
            if not content["response"].get("body"):
                break
            item_list = content["response"]["body"]["items"]["item"]
            for wrn_item in item_list:
                ts = wrn_item["tmFc"]
                timestamp_ts = datetime.strptime(ts, "%Y.%m.%d.%H:%M")
                latest_timestamp = max(latest_timestamp, timestamp_ts)
                if timestamp_ts <= timestamp_updated_at:
                    break
                jsonl_str += json.dumps(wrn_item, ensure_ascii=False) + "\n"
        ti.xcom_push(key="updated_at", value=latest_timestamp)
        return jsonl_str

    def paginate(response: Response) -> dict | None:
        content = response.json()
        if not content["response"].get("body"):
            return None
        body = content["response"]["body"]
        total_count = body["totalCount"]
        cur_page_no = body["pageNo"]
        cur_num_of_rows = body["numOfRows"]
        if cur_page_no * cur_num_of_rows < total_count:
            return dict(params={"pageNo": cur_page_no + 1, })

    kat_real_time_to_gcs = PublicDataToGCSOperator(
        task_id="extract_kma_wrn",
        bucket_name="bomnet-raw",
        data={
            "pageNo": 1,
            "numOfRows": 100,
            "dataType": "json"
        },
        object_name=f"kma/wrn/{{ ds_nodash }}.jsonl",
        endpoint="/1360000/WthrWrnInfoService/getWthrWrnList",
        response_filter=response_filter,
        pagination_function=paginate,
        api_type=("query", "serviceKey")
    )

    kat_real_time_to_gcs


extract_mafra_kat_real_time()
