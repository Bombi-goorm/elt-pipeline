from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from airflow.decorators import dag, task


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 18),
    render_template_as_native_obj=True,
    catchup=False,
)
def match_price_condition():
    @task
    def fetch_data_from_mysql():
        mysql_hook = MySqlHook(mysql_conn_id="mysql_test_connection")
        sql = "SELECT * FROM notification_condition;"
        records = mysql_hook.get_records(sql)
        for record in records:
            print(record)

    fetch_data_from_mysql()


match_price_condition()
