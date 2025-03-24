from include.custom_operators.data_go_abc import PublicDataToGCSOperator
from datetime import datetime
import json


class MafraAuctionToGCSOperator(PublicDataToGCSOperator):
    def __init__(self,
                 start_index: int,
                 end_index: int,
                 whsal_cd: str,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.start_index = start_index
        self.end_index = end_index
        self.whsal_cd = whsal_cd

    def execute(self, context):

        response = self.fetch_public_data(context['ds_nodash'])
        jsonl_list = self.process_json(response)
        jsonl_str = ""
        object_name = f"mafra/auction/{context['ds_nodash']}.jsonl"

        updated_at = context["ti"].xcom_pull(key="auction_updated_at", default="2025-01-01 00:00:00")
        timestamp_updated_at = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        latest_timestamp = jsonl_list[0]["SBIDTIME"]

        for auction in jsonl_list:
            sbidtime = auction["SBIDTIME"]
            timestamp_sbidtime = datetime.strptime(sbidtime, "%Y-%m-%d %H:%M:%S")

            if timestamp_sbidtime <= timestamp_updated_at:
                break
            auction_filtered = {key: value for key, value in auction.items() if key != "ROW_NUM"}
            jsonl_str += json.dumps(auction_filtered, ensure_ascii=False) + "\n"

        context["ti"].xcom_push(key="updated_at", value=latest_timestamp)

        self.upload_to_gcs(jsonl_str, object_name)

    def process_json(self, json_data) -> list:
        try:
            rows = json_data["Grid_20240625000000000654_1"]["row"]
            return rows
        except KeyError as e:
            raise Exception("JSON 응답 형식이 다릅니다.")

    def build_url(self, api_key, ds_nodash):
        query_params = (f"SALEDATE={ds_nodash}&"
                        f"WHSALCD={self.whsal_cd}")
        return f"openapi/{api_key}/json/Grid_20240625000000000654_1/1/1000/?{query_params}"
