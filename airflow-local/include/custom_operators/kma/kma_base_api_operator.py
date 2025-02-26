from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json


class KmaBaseApiOperator(BaseOperator, ABC):
    template_fields = ("ds",)

    def __init__(self, page_no: int,
                 num_of_rows: int,
                 bucket_name: str,
                 ds_nodash: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_no = page_no
        self.num_of_rows = num_of_rows
        self.bucket_name = bucket_name
        self.ds_nodash = ds_nodash

    def execute(self, context):
        http_hook = HttpHook(http_conn_id='kma-connection', method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        extra = conn.extra_dejson
        api_key = extra['api_key']
        response = http_hook.run(endpoint=self.build_url(api_key, context['ds_nodash']))

        if response.status_code != 200:
            raise Exception(f"API 요청 실패: {response.status_code}")

        object_name = self.generate_object_name(context['ds_nodash'])
        json_data = response.json()
        jsonl_list = self.process_json(json_data)
        jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in jsonl_list])
        self.upload_to_gcs(jsonl_str, object_name)

    @staticmethod
    def process_json(json_data):
        try:
            return json_data["response"]["body"]["items"]["item"]
        except KeyError:
            raise Exception("JSON 응답 형식이 다릅니다.")

    @abstractmethod
    def build_url(self, api_key, ds_nodash):
        pass

    @abstractmethod
    def generate_object_name(self, ds_nodash):
        pass

    def upload_to_gcs(self, jsonl_str, object_name):
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=jsonl_str,
            mime_type='application/json'
        )
        self.log.info(f"Uploaded to GCS: gs://{self.bucket_name}/{object_name}")
