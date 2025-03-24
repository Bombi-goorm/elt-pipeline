from datetime import datetime
from cosmos.constants import LoadMode

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, DbtTaskGroup
from cosmos.constants import ExecutionMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.decorators import dag, task
from os import environ
from airflow.datasets import Dataset

DBT_PROJECT_PATH = f"{environ['AIRFLOW_HOME']}/dags/dbt"
short_dataset = Dataset("bigquery://bomnet.short")

profile_config = ProfileConfig(
    profile_name="bigquery-db",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp-sample",
        profile_args={"project": "goorm-bomnet", "dataset": "kma"}
    ),
)


@dag(
    start_date=datetime(2024, 2, 18),
    schedule=[short_dataset],
    catchup=False,
)
def transform_kma_short():
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = BashOperator(
        task_id="end-venv-examples",
        bash_command="echo hello",
    )
    transform_short = DbtTaskGroup(
        group_id="transform_short",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
        ),
        render_config=RenderConfig(
            select=["stg_kma__short+"],
            load_method=LoadMode.DBT_MANIFEST
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-bigquery"],  # 또는 사용하는 adapter에 맞춰 변경
            "install_deps": True
        }
    )

    start_task >> transform_short >> end_task


transform_kma_short()
