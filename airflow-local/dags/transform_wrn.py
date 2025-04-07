from datetime import datetime
from cosmos.constants import LoadMode

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup
from cosmos.constants import ExecutionMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.decorators import dag, task
from airflow.datasets import Dataset

DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt"
wrn_dataset = Dataset("bigquery://bomnet.wrn")
dbt_wrn = Dataset("bigquery://bomnet.transform.wrn")
profile_config = ProfileConfig(
    profile_name="bigquery-db",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud_bomnet_conn",
        profile_args={"project": "goorm-bomnet", "dataset": "kma"}
    ),
)


@dag(
    start_date=datetime(2024, 2, 18),
    schedule=[wrn_dataset],
    catchup=False,
)
def transform_kma_wrn():
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = BashOperator(
        task_id="end-venv-examples",
        bash_command="echo hello",
        outlets=[dbt_wrn]
    )
    transform_wrn = DbtTaskGroup(
        group_id="transform_wrn",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV
        ),
        render_config=RenderConfig(
            select=["stg_kma__wrn+"],
            load_method=LoadMode.DBT_MANIFEST
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-bigquery"],
            "install_deps": True
        }
    )

    start_task >> transform_wrn >> end_task


transform_kma_wrn()
