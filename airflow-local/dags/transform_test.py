from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.datasets import Dataset

from cosmos import DbtDag, DbtTaskGroup
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt"
short_dataset = Dataset("bigquery://bomnet.short")

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
    schedule=[short_dataset],
    catchup=False,
)
def test119():
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = BashOperator(
        task_id="end-venv-examples",
        bash_command="cd /home/airflow/gcs/dags/dbt && dbt compile",
    )
    test_transform = DbtTaskGroup(
        group_id="test_transform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
        ),
        render_config=RenderConfig(
            # select=["stg_kma__short+"],
            load_method=LoadMode.DBT_MANIFEST
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-bigquery"],
            "install_deps": True
        }
    )

    start_task >> test_transform >> end_task


test119()
