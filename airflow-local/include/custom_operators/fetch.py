from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.context import Context
import json


class MafraKatSaleFetchOperator(HttpOperator):

    def __init__(self, whsl_mrkt_cd_list: list, date: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.whsl_mrkt_cd_list = whsl_mrkt_cd_list
        self.date = date

    def execute(self, context: Context):
        results = []

        for code in self.whsl_mrkt_cd_list:
            # ✅ API 호출
            response = super().execute(context)
            json_data = json.loads(response.text)

            total_count = int(json_data["response"]["body"]["totalCount"]) if "body" in json_data["response"] else 0

            # ✅ totalCount=0이면 스킵 (태스크는 실패 X)
            if total_count == 0:
                self.log.info(f"❌ 도매시장 {code}: totalCount=0 (건너뜀)")
                continue

            # ✅ 페이지 계산 후 리스트에 추가
            num_pages = (total_count + 999) // 1000  # 1000개 단위 페이지 계산
            for page in range(1, num_pages + 1):
                results.append({"whsl_mrkt_cd": code, "pageNo": page})

        return results  # ✅ `expand_kwargs()`에서 바로 사용 가능!
