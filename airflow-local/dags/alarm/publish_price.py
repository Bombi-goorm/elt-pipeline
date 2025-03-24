from pendulum import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
import json


@dag(
    schedule_interval=None,
    start_date=datetime(2025, 2, 18),
    catchup=False,
)
def publish_price():
    @task(task_id="fetch_bigquery_data")
    def fetch_bigquery_data():
        hook = BigQueryHook(
            task_id="fetch_bigquery_data",
            gcp_conn_id="gcp-sample",
            location="asia-northeast3",
        )
        sql = "SELECT * FROM kma.int_aws__match_price_condition"

        records = hook.get_records(sql)
        if not records:
            return []

        fields = ["member_id",
                  "target_price",
                  "matched_price",
                  "variety",
                  "whsl_mrkt_nm",
                  "price_direction",
                  "scsbd_dt"]
        messages = [dict(zip(fields, record)) for record in records]

        return messages

    @task
    def publish_to_pubsub(messages):
        if not messages:
            return "No new messages to publish"

        pubsub_hook = PubSubHook(gcp_conn_id="gcp-sample")
        topic = "bomnet-test"
        pubsub_messages = [
            {"data": json.dumps(msg, ensure_ascii=False).encode("utf-8")}
            for msg in messages
        ]
        print(pubsub_messages)
        pubsub_hook.publish(topic=topic, messages=pubsub_messages)
        print(f"Published {len(messages)} messages to Pub/Sub")

    messages = fetch_bigquery_data()
    publish_to_pubsub(messages)


publish_price()
