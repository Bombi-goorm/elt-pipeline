from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class KmaAbstractOperator(BaseOperator, ABC):

    def __init__(self, page_no: int,
                 num_of_rows: int,
                 bucket_name: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_no = page_no
        self.num_of_rows = num_of_rows
        self.bucket_name = bucket_name

    @abstractmethod
    def execute(self, context):
        pass

    @staticmethod
    def process_json(json_data):
        try:
            return json_data["response"]["body"]["items"]["item"]
        except KeyError:
            raise Exception("JSON 응답 형식이 다릅니다.")

    @abstractmethod
    def build_url(self, api_key, ds_nodash):
        pass

    def upload_to_gcs(self, jsonl_str, object_name):
        gcs_hook = GCSHook(gcp_conn_id='gcp-sample')
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=jsonl_str,
            mime_type='application/json'
        )
        self.log.info(f"Uploaded to GCS: gs://{self.bucket_name}/{object_name}")

    def request_kma(self, ds_nodash):
        http_hook = HttpHook(http_conn_id='kma-connection', method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        extra = conn.extra_dejson
        api_key = extra['api_key']
        response = http_hook.run(endpoint=self.build_url(api_key, ds_nodash))

        if response.status_code != 200:
            raise Exception(f"API 요청 실패: {response.status_code}")
        return response.json()