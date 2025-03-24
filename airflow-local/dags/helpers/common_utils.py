from requests import Response
from typing import Optional, List, Union
import json
from json import JSONDecodeError
from airflow.exceptions import AirflowBadRequest


def datago_validate_api_response(response: Response) -> bool:
    try:
        json_data = response.json()
    except JSONDecodeError:
        raise AirflowBadRequest("API Response is not valid JSON")

    header = json_data["response"]["header"]
    if header["resultCode"] != "0":
        raise AirflowBadRequest(f"Error {header["resultCode"]}: {header["resultMsg"]}")
    total_count = json_data["response"]["body"]["totalCount"]
    print(f"Total Count : {json_data["response"]}")
    return False if total_count == 0 else True


def datago_paginate(response: Response) -> dict | None:
    content = response.json()
    if not content["response"].get("body"):
        return None
    body = content["response"]["body"]
    total_count = body["totalCount"]
    cur_page_no = body["pageNo"]
    cur_num_of_rows = body["numOfRows"]
    if cur_page_no * cur_num_of_rows < total_count:
        return dict(params={"pageNo": cur_page_no + 1, })


def datago_safe_response_filter(responses: Union[Response, List[Response]]) -> Optional[str]:
    jsonl_list = []
    for res in responses if isinstance(responses, list) else [responses]:
        if not res.text.strip():
            raise ValueError("API returned an empty response")
        content = res.json()
        total_count = content["response"]["body"]["totalCount"]
        if total_count == 0:
            return None
        item_list = content["response"]["body"]["items"]["item"]
        jsonl_list.extend(json.dumps(item, ensure_ascii=False) for item in item_list)
    return "\n".join(jsonl_list) if jsonl_list else None
