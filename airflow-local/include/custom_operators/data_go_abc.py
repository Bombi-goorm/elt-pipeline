from abc import ABC, abstractmethod
from typing import Sequence

from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from typing import Any, Callable, Optional
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


class PublicDataToGCSOperator(BaseOperator, ABC):
    template_fields: Sequence[str] = ("bucket_name",)

    def __init__(self,
                 bucket_name: str,
                 target_object: str,
                 endpoint: str | None = None,
                 method: str = "GET",
                 data: dict[str, Any] | str | None = None,
                 headers: dict[str, str] | None = None,
                 response_check: Callable[..., bool] | None = None,
                 response_filter: Callable[..., Any] | None = None,
                 http_conn_id: str = "datago_connection",
                 gcp_conn_id: str = "gcp-sample",
                 mime_type: str = "application/json",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.target_object = target_object
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.data = data
        self.response_check = response_check
        self.response_filter = response_filter
        self.http_conn_id = http_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type

    def execute(self, context):
        pass

    def process_response(json_data):
        try:
            return_value = json_data["response"]["body"]["items"]["item"]
            if not return_value:
                print("NO RESPONSE!!!!!!")
            return return_value
        except KeyError:
            raise Exception("JSON Key Error")

    def upload_to_gcs(self, jsonl_str: str):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=self.target_object,
            data=jsonl_str,
            mime_type=self.mime_type,
        )
        self.log.info(f"Uploaded to GCS: gs://{self.bucket_name}/{self.target_object}")

    def fetch_public_data(self):
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        api_key = conn.extra_dejson['api_key']
        self.data["serviceKey"] = api_key
        # api 키를 쿼리 파라미터에 추가할 경우와 요청 파라미터에 그냥 추가하는 경우 다르게 해야 함.
        # 응답에 대한 에러처리는 response check에서 할 것.

        response = http_hook.run(endpoint=self.endpoint, data=self.data, headers=self.headers)

        self.log.debug("Sending '%s' to url: %s", self.method, self.endpoint)

        # response check에서 에러나도록 할 것
        # if response.status_code != 200:
        #     raise Exception(f"Failed to fetch data from {self.endpoint}, status code: {response.status_code}")
        return response.json()
