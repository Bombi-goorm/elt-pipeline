import requests
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.http.sensors.http import HttpSensor


class MafraKatSaleSensor(BaseSensorOperator):

    def __init__(self, url: str, expected_status_code: int = 200, response_check=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.expected_status_code = expected_status_code
        self.response_check = response_check  # Optional function to check response body

    def poke(self, context):
        """Makes an API request and checks if the condition is met."""
        self.log.info(f"Checking API: {self.url}")
        try:
            response = requests.get(self.url)
            if response.status_code != self.expected_status_code:
                self.log.info(f"Received status {response.status_code}, expected {self.expected_status_code}")
                return False

            # If a response_check function is provided, use it to validate response
            if self.response_check:
                return self.response_check(response)

            return True  # Default: status code is enough to succeed

        except requests.RequestException as e:
            self.log.error(f"HTTP request failed: {e}")
            return False
