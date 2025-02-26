import json
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator
from typing import List, Dict


class KmaShortApiOperator(BaseOperator):
    def __init__(self,
                 # nx: int,
                 # ny: int,
                 base_time: str,
                 page_no: int,
                 num_of_rows: int,
                 xy_pair: tuple,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        # self.nx = nx
        # self.ny = ny
        self.xy_pair = xy_pair
        self.base_time = base_time
        self.page_no = page_no
        self.num_of_rows = num_of_rows

    def execute(self, context):
        http_hook = HttpHook(http_conn_id='kma-connection', method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        extra = conn.extra_dejson
        api_key = extra['api_key']
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.page_no}&"
                        f"base_date={context["ds_nodash"]}&"
                        f"numOfRows={self.num_of_rows}&"
                        f"dataType=json&"
                        f"base_time={self.base_time}&"
                        f"nx={self.xy_pair[0]}&ny={self.xy_pair[1]}")

        url = f"/1360000/VilageFcstInfoService_2.0/getVilageFcst?{query_params}"
        response = http_hook.run(endpoint=url)

        if response.status_code != 200:
            self.log.error(f"API 요청 실패: {response.status_code}")
            raise Exception(f"API 요청 실패: {response.status_code}")

        json_data = response.json()
        processed_data = self.__process_json_data(json_data)

        return processed_data

    def __process_json_data(self, json_data) -> str:
        try:
            response = json_data["response"]
            header, body = response["header"], response["body"]
            if header["resultCode"] == 99:
                raise Exception()

            items = body["items"]

            return items
        except KeyError as e:
            self.log.error(f"JSON 형식이 예상과 다릅니다. 예외 발생: {e}")
            raise
        except Exception as e:
            self.log.error(f"데이터 처리 중 오류 발생: {e}")
            raise
