from requests import Response
from typing import Optional, List, Union
import json
from json import JSONDecodeError
from airflow.exceptions import AirflowBadRequest
import time
import logging
import sys
import functools
import json


def ensure_log_flush(func):
    """
    Decorator to guarantee log flushing and basic execution logging.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_id = kwargs.get('task_id', func.__name__)
        logging.info(f"[{task_id}] Start - args: {args} kwargs: {json.dumps(kwargs, default=str)}")
        sys.stdout.flush()

        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = round(time.time() - start, 3)
            logging.info(f"[{task_id}] Success in {duration}s")
            return result
        except Exception as e:
            logging.exception(f"[{task_id}] Failed with error: {e}")
            raise
        finally:
            sys.stdout.flush()
            time.sleep(1)  # Let logs propagate

    return wrapper


def datago_validate_api_response(response: Response) -> bool:
    try:
        json_data = response.json()
    except JSONDecodeError:
        raise AirflowBadRequest("API Response is not valid JSON")

    header = json_data["response"]["header"]
    if header["resultCode"] != "0":
        raise AirflowBadRequest(f"Error {header['resultCode']}: {header['resultMsg']}")
    total_count = json_data["response"]["body"]["totalCount"]
    print(f"Total Count : {json_data['response']}")
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
