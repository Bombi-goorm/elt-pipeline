from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from datetime import datetime
import json

GCP_PROJECT_ID = "goorm-bomnet"
PUBSUB_TOPIC = "bomnet-test"

market_pairs = [
    ["서울가락", "대전중앙"], ["부산반여", "대구매천"], ["대구매천", "부산엄궁"],
    ["서울강서", "대전오정"], ["대전오정", "광주서부"], ["대구매천", "광주각하"],
    ["부산엄궁", "서울강서"], ["춘천도매", "서울가락"], ["제주도매", "서울가락"],
    ["인천구월", "수원팔달"], ["수원팔달", "천안중앙"], ["청주복합", "전주송정"],
    ["전주서부", "대구매천"], ["인천삼산", "안산농수산"], ["안산농수산", "수원농수산"],
    ["수원농수산", "서울가락"], ["용인농수산", "이천농수산"], ["이천농수산", "여주농수산"],
    ["김해농수산", "진주중앙"], ["진주중앙", "창원팔용"],
]

products = [
    "배", "감귤", "양파", "양배추", "당근", "딸기", "블루베리", "오렌지",
    "감자", "고구마", "옥수수", "참외", "수박", "멜론", "양파대파", "깻잎",
    "상추", "브로콜리", "토마토", "애호박", "파프리카"
]

messages = []
for idx, (markets, product) in enumerate(zip(market_pairs, products), start=1):
    message_data = {
        "user_id": f"user_{idx}",
        "product": product,
        "price": "3000 +inf",
        "markets": markets
    }
    messages.append({"data": json.dumps(message_data, ensure_ascii=False).encode("utf-8")})

# DAG 정의
default_args = {
    "start_date": datetime(2025, 3, 7),
    "catchup": False
}

with DAG(
        dag_id="send_to_pubsub",
        schedule_interval="*/1 * * * *",  # 매 1분마다 실행
        default_args=default_args,
        tags=["pubsub", "airflow"],
) as dag:
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_to_pubsub",
        gcp_conn_id="gcp-sample",
        project_id=GCP_PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=messages
    )

    publish_task
