from requests import Response


def paginate(response: Response) -> dict | None:
    content = response.json()
    if not content["response"].get("body"):
        return None
    body = content["response"]["body"]
    total_count = body["totalCount"]
    cur_page_no = body["pageNo"]
    cur_num_of_rows = body["numOfRows"]
    if cur_page_no * cur_num_of_rows < total_count:
        return dict(params={"pageNo": cur_page_no + 1, })
