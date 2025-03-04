from include.custom_operators.kma.kma_abc import KmaAbstractOperator
import json

class KmaShortApiOperator(KmaAbstractOperator):
    def __init__(self, base_time: str, xy_pair: tuple, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_time = base_time
        self.xy_pair = xy_pair

    def execute(self, context):
        response = self.request_kma(context['ds_nodash'])
        object_name = f"kma/short/{context['ds_nodash']}/{self.xy_pair[0]}_{self.xy_pair[1]}.jsonl"
        jsonl_list = self.process_json(response)
        jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in jsonl_list])
        self.upload_to_gcs(jsonl_str, object_name)

    def build_url(self, api_key, ds_nodash):
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.page_no}&"
                        f"base_date={ds_nodash}&"
                        f"numOfRows={self.num_of_rows}&"
                        f"dataType=json&"
                        f"base_time={self.base_time}&"
                        f"nx={self.xy_pair[0]}&ny={self.xy_pair[1]}")
        return f"/1360000/VilageFcstInfoService_2.0/getVilageFcst?{query_params}"

