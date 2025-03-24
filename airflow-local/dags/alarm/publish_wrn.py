from pendulum import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
import json


@dag(
    start_date=datetime(2025, 2, 18),
    catchup=False,
)
def publish_wrn():
    @task(task_id="fetch_bigquery_data")
    def fetch_bigquery_data():
        hook = BigQueryHook(
            task_id="fetch_bigquery_data",
            gcp_conn_id="gcp-sample",
            location="asia-northeast3",
        )
        sql = "SELECT stn_nm, title FROM kma.int_kma__wrn_alarm"

        records = hook.get_records(sql)
        if not records:
            return []

        fields = ["station_id",
                  "title",
                  "fcst_date_time",
                  ]
        messages = [dict(zip(fields, record)) for record in records]

        return messages

    @task
    def publish_to_pubsub(messages):
        if not messages:
            return "No new messages to publish"

        pubsub_hook = PubSubHook(gcp_conn_id="gcp-sample")
        topic = "bomnet-wrn-topic"
        pubsub_messages = [
            {"data": json.dumps(msg, ensure_ascii=False).encode("utf-8")}
            for msg in messages
        ]
        print(pubsub_messages)
        pubsub_hook.publish(topic=topic, messages=pubsub_messages)
        print(f"Published {len(messages)} messages to Pub/Sub")

    messages = fetch_bigquery_data()
    publish_to_pubsub(messages)


publish_wrn()
