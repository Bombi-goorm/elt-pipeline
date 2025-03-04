import json
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator
from typing import List


class MafraApiOperator(BaseOperator):
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
        http_hook = HttpHook(http_conn_id='mafra-connection', method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        extra = conn.extra_dejson
        api_key = extra['api_key']
        query_params = f"SALEDATE={context["ds_nodash"]}&WHSALCD={self.whsal_cd}"
        url = f"openapi/{api_key}/json/Grid_20240625000000000654_1/{self.start_index}/{self.end_index}?{query_params}"

        response = http_hook.run(endpoint=url)

        if response.status_code != 200:
            self.log.error(f"API 요청 실패: {response.status_code}")
            raise Exception(f"API 요청 실패: {response.status_code}")

        json_data = response.json()

        jsonl_data = self.__process_json_data(json_data, ["ROW_NUM"])

        return jsonl_data

    def __process_json_data(self, json_data, exclude_keys: List[str] = None) -> str:
        exclude_keys = exclude_keys or []

        try:
            rows = json_data["Grid_20240625000000000654_1"]["row"]

            filtered_rows = [
                {k: v for k, v in row.items() if k not in exclude_keys}
                for row in rows
            ]

            jsonl_data = "\n".join([json.dumps(row, ensure_ascii=False) for row in filtered_rows])

            return jsonl_data
        except KeyError as e:
            self.log.error(f"JSON 형식이 예상과 다릅니다. 예외 발생: {e}")
            raise
        except Exception as e:
            self.log.error(f"데이터 처리 중 오류 발생: {e}")
            raise
