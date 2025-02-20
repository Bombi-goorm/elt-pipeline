import requests


class APIClient:
    def __init__(self, base_url, api_key, api_url, start_index, end_index, params=None):
        self.url = f"{base_url}/{api_key}/json/{api_url}/{start_index}/{end_index}"
        self.params = params or {}

    def send_request(self):
        try:
            response = requests.get(self.url, params=self.params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API 요청 실패: {e}")
            return None
