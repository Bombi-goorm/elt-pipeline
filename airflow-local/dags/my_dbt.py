from datetime import datetime
from cosmos import DbtDag, DbtTaskGroup
from cosmos.constants import ExecutionMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

profile_config = ProfileConfig(
    profile_name="bigquery-db",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp-sample"
    ),
)
my_dbt_dag = DbtDag(
    profile_config=profile_config,
    exection_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV,
    ),
    operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["apache-airflow-providers-bigquery"],
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 1},
)
