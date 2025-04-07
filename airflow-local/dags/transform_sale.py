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

DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt"
sale_dataset = Dataset("bigquery://bomnet.sale")

profile_config = ProfileConfig(
    profile_name="bigquery-db",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud_bomnet_conn",
        profile_args={"project": "goorm-bomnet", "dataset": "kma"}
    ),
)

dbt_realtime = Dataset("bigquery://bomnet.transform.realtime")


@dag(
    start_date=datetime(2024, 2, 18),
    schedule=[sale_dataset],
    catchup=False,
)
def transform_kma_sale():
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = BashOperator(
        task_id="end-venv-examples",
        bash_command="echo hello",
        outlets=[dbt_realtime]
    )
    transform_sale = DbtTaskGroup(
        group_id="transform_sale",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
        ),
        render_config=RenderConfig(
            select=["stg_mafra_kat_sale+"],
            load_method=LoadMode.DBT_MANIFEST
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-bigquery"],
            "install_deps": True
        }
    )

    start_task >> transform_sale >> end_task


transform_kma_sale()
