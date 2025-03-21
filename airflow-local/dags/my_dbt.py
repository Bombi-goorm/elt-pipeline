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

DBT_PROJECT_PATH = f"{environ['AIRFLOW_HOME']}/dags/dbt"

profile_config = ProfileConfig(
    profile_name="bigquery-db",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp-sample",
        profile_args={"project": "goorm-bomnet", "dataset": "kma"}
    ),
)


@dag(
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def my_simple_dbt_dag():
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = BashOperator(
        task_id="end-venv-examples",
        bash_command="echo hello",
    )
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
        ),
        render_config=RenderConfig(
            select=["stg_mafra_kat_sale"],
            load_method=LoadMode.DBT_MANIFEST
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-bigquery"],  # 또는 사용하는 adapter에 맞춰 변경
            "install_deps": True
        }
    )

    start_task >> transform_data >> end_task


my_simple_dbt_dag()
