from include.custom_operators.data_go_abc import PublicDataToGCSOperator
import json


class MafraKatSaleToGCSOperator(PublicDataToGCSOperator):

    def __init__(self,
                 pageNo,
                 numOfRows,
                 whsl_mrkt_cd,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pageNo = pageNo
        self.numOfRows = numOfRows
        self.whsl_mrkt_cd = whsl_mrkt_cd

    def execute(self, context):
        print(context['yesterday_ds'])
        print(context['ds'])
        print(context['ts'])
        response = self.fetch_public_data(context['yesterday_ds'])
        object_name = f"mafra/kat_sale/{self.whsl_mrkt_cd}/{context['ds_nodash']}.jsonl"

        jsonl_list = self.process_json(response)
        jsonl_str = "\n".join([json.dumps(item, ensure_ascii=False) for item in jsonl_list])
        self.upload_to_gcs(jsonl_str, object_name)

    def build_url(self, api_key, ds):
        query_params = (f"serviceKey={api_key}&"
                        f"pageNo={self.pageNo}&"
                        f"numOfRows={self.numOfRows}&"
                        f"cond[trd_clcln_ymd::EQ]={"2025-03-07"}&"
                        f"cond[whsl_mrkt_cd::EQ]={self.whsl_mrkt_cd}")
        return f"B552845/katSale/trades?{query_params}"
