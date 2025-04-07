from typing import Sequence
from airflow.datasets import Dataset
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from typing import Any, Callable
from requests import Response
from airflow.utils.context import Context
from airflow.exceptions import AirflowSkipException


class PublicDataToGCSOperator(BaseOperator):
    template_fields: Sequence[str] = ("bucket_name", "object_name", "data")

    def __init__(self,
                 bucket_name: str,
                 object_name: str,
                 endpoint: str | None = None,
                 method: str = "GET",
                 data: dict[str, Any] | str | None = None,
                 expanded_data: dict = None,
                 headers: dict[str, str] | None = None,
                 pagination_function: Callable[..., Any] | None = None,
                 response_check: Callable[..., bool] | None = None,
                 response_filter: Callable[..., Any] | None = None,
                 api_type=None,
                 http_conn_id: str = "datago_connection",
                 gcp_conn_id: str = "google_cloud_bomnet_conn",
                 mime_type: str = "application/json",
                 alias_name: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.pagination_function = pagination_function
        self.data = data
        self.expanded_data = expanded_data
        self.response_check = response_check
        self.response_filter = response_filter
        self.api_type = api_type
        self.http_conn_id = http_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.alias_name = alias_name

    def execute(self, context: Context):
        self.log.info("Calling HTTP method")
        processed_response = self._fetch_public_data(context)
        if processed_response:
            self.upload_to_gcs(processed_response)
            if self.alias_name:
                print(f"alias name {self.alias_name} is set!!, execution time is {context['ds']}")
                dataset_uri = f"gcs://{self.bucket_name}/{self.object_name}"
                context["outlet_events"][self.alias_name].add(Dataset(dataset_uri), extra={"ds": context['ds']})
        else:
            raise AirflowSkipException("There is no response. so skip it.")

    def process_response(self, context: Context, response: Response | list[Response]) -> Any:
        from airflow.utils.operator_helpers import determine_kwargs

        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs = determine_kwargs(self.response_filter, [response], context)
            print(kwargs)
            return self.response_filter(response, **kwargs)

    def upload_to_gcs(self, jsonl_str: str):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=jsonl_str,
            mime_type=self.mime_type,
        )
        self.log.info(f"Uploaded to GCS: gs://{self.bucket_name}/{self.object_name}")

    def _fetch_public_data(self, context: Context) -> Any:
        if self.expanded_data:
            self.data.update(self.expanded_data)
            for key, value in self.expanded_data.items():
                self.object_name += f"{value}_"
            self.object_name += ".jsonl"
        self.log.info(f"data: {self.data}")
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method='GET')
        conn = http_hook.get_connection(http_hook.http_conn_id)
        api_key: str = conn.extra_dejson['api_key']
        self.data[self.api_type[1]] = api_key

        response = http_hook.run(endpoint=self.endpoint, data=self.data, headers=self.headers)

        all_responses = self.paginate_sync(http_hook, response=response)

        return self.process_response(context=context, response=all_responses)

    def paginate_sync(self, hook: HttpHook, response: Response) -> Response | list[Response]:
        if not self.pagination_function:
            return response

        all_responses = [response]
        while True:
            next_page_params = self.pagination_function(response)
            print(f"next_page_params: {next_page_params}")
            if not next_page_params:
                break
            merged_page_parameters = self._merge_next_page_parameters(next_page_params)
            print(f"merged_page_parameters: {merged_page_parameters}")
            response = hook.run(**merged_page_parameters)
            all_responses.append(response)
        return all_responses

    def _merge_next_page_parameters(self, next_page_params: dict) -> dict:
        return dict(
            endpoint=next_page_params.get("endpoint") or self.endpoint,
            data={**self.data, **next_page_params.get("params", {})},
            headers={**self.headers, **next_page_params.get("headers", {})},
        )
