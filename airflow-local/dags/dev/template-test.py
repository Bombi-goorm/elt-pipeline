# from airflow import DAG
# from airflow.models import BaseOperator
# from airflow.utils.dates import days_ago
# from airflow.utils.decorators import apply_defaults
# from datetime import datetime
# xy_combinations = [
#     (38, 53),
#     (74, 97),
#     (75, 57)
# ]
#
# # 템플릿을 지원하는 커스텀 오퍼레이터
# class SimpleTemplateOperator(BaseOperator):
#     # 템플릿을 지원할 필드 지정
#     template_fields = ("file_path",)
#
#     @apply_defaults
#     def __init__(self, file_path, xy_pair, **kwargs):
#         super().__init__(**kwargs)
#         self.file_path = file_path  # 템플릿이 지원될 파라미터
#         self.xy_pair = xy_pair
#     def execute(self, context):
#         # file_path는 템플릿 필드이므로 실행 시점에 자동으로 렌더링됨
#         resolved_file_path = self.file_path
#         print(f"Resolved file path: {resolved_file_path} {self.xy_pair}")
#
# # DAG 정의
# default_args = {
#     "owner": "airflow",
#     "start_date": days_ago(1),
# }
#
# with DAG(
#     dag_id="simple_template_dag",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False,
# ) as dag:
#
#     # 커스텀 오퍼레이터 사용
#     simple_task = SimpleTemplateOperator.partial(
#         task_id="simple_task",
#         file_path="{{ ds_nodash }}{{ params.xy_pair[0] }}_{{ params.xy_pair[1] }}.json",  # 템플릿 사용
#     ).expand(xy_pair=xy_combinations)