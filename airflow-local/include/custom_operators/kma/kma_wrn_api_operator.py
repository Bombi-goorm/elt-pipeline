from include.custom_operators.data_go_abc import PublicDataToGCSOperator
import re
import json
from datetime import datetime


class KmaWrnToGCSOperator(PublicDataToGCSOperator):
    def __init__(self, page_no: int,
                 num_of_rows: int,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_no = page_no
        self.num_of_rows = num_of_rows

    def execute(self, context):
        response = self.fetch_public_data('datago_connection', context['ds_nodash'])
        object_name = f"kma/wrn/{context['ds_nodash']}.jsonl"
        jsonl_list = self.process_json(response)

        jsonl_str = ""
        latest_timestamp = self.extract_timestamp(jsonl_list[0]["title"])
        self.log.error(f"latest_timestamp: {latest_timestamp}")

        updated_at = context["ti"].xcom_pull(key="updated_at", default="2025.01.01.00:00")
        timestamp_updated_at = datetime.strptime(updated_at, "%Y.%m.%d.%H:%M")

        for wrn_item in jsonl_list:
            title = wrn_item["title"]
            ts = self.extract_timestamp(title)
            self.log.error(f"updated_at: {updated_at}")
            self.log.error(f"ts: {ts}")

            timestamp_ts = datetime.strptime(ts, "%Y.%m.%d.%H:%M")

            if timestamp_ts <= timestamp_updated_at:
                break
            jsonl_str += json.dumps(wrn_item, ensure_ascii=False) + "\n"
        context["ti"].xcom_push(key="updated_at", value=latest_timestamp)

        self.upload_to_gcs(jsonl_str, object_name)

    def build_url(self, api_key, ds_nodash):
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.page_no}&"
                        f"fromTmFc={ds_nodash}&"
                        f"numOfRows={self.num_of_rows}&"
                        f"dataType=json")
        return f"/1360000/WthrWrnInfoService/getWthrWrnList?{query_params}"

    @staticmethod
    def extract_timestamp(title: str):
        pattern = r"\d{4}\.\d{2}\.\d{2}\.\d{2}:\d{2}"
        timestamp = re.search(pattern, title).group()
        return timestamp
