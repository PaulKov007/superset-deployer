import requests
from functools import cached_property

from SupersetApiClient.charts import Charts
from SupersetApiClient.dashboards import Dashboards
from SupersetApiClient.datasets import Datasets


class SupersetClient:

    def __init__(self, api_endpoint: str, username: str, password: str):
        self.api_endpoint = api_endpoint
        self.username = username
        self.password = password

        self.dashboards = Dashboards(self)
        self.datasets = Datasets(self)
        self.charts = Charts(self)

    @cached_property
    def session(self) -> requests.sessions.Session:
        api_session = requests.session()
        jwt_token = api_session.post(
            url=f"{self.api_endpoint}/security/login",
            json={
                "username": self.username,
                "password": self.password,
                "refresh": False,
                "provider": "db"
            }
        ).json()["access_token"]

        csrf_token = api_session.get(
            url=f"{self.api_endpoint}/security/csrf_token/",
            headers={'Authorization': f'Bearer {jwt_token}'}
        ).json()["result"]

        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {jwt_token}',
            'X-CSRFToken': csrf_token,
        }
        api_session.headers.update(headers)
        return api_session

    @property
    def get(self):
        return self.session.get

    @property
    def post(self):
        return self.session.post

    @property
    def put(self):
        return self.session.put

    @property
    def delete(self):
        return self.session.delete


