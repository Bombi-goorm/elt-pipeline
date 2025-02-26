from include.custom_operators.kma.kma_base_api_operator import KmaBaseApiOperator


class KmaWrnApiOperator(KmaBaseApiOperator):

    def build_url(self, api_key, ds_nodash):
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.page_no}&"
                        f"fromTmFc={ds_nodash}&"
                        f"numOfRows={self.num_of_rows}&"
                        f"dataType=json")
        return f"/1360000/WthrWrnInfoService/getWthrWrnList?{query_params}"

    def generate_object_name(self, ds_nodash):
        return f"{ds_nodash}.jsonl"
