from include.custom_operators.kma.kma_base_api_operator import KmaBaseApiOperator


class KmaShortApiOperator(KmaBaseApiOperator):
    def __init__(self, base_time: str, xy_pair: tuple, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_time = base_time
        self.xy_pair = xy_pair

    def build_url(self, api_key, ds_nodash):
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.page_no}&"
                        f"base_date={ds_nodash}&"
                        f"numOfRows={self.num_of_rows}&"
                        f"dataType=json&"
                        f"base_time={self.base_time}&"
                        f"nx={self.xy_pair[0]}&ny={self.xy_pair[1]}")
        return f"/1360000/VilageFcstInfoService_2.0/getVilageFcst?{query_params}"

    def generate_object_name(self, ds_nodash):
        return f"{ds_nodash}/{self.xy_pair[0]}_{self.xy_pair[1]}.jsonl"
