import json

import requests
import yaml
import os
import io
from functools import cached_property
from SupersetApiClient.data_model import DataModel


class ApiObject:
    object_type: str = "unknown"
    data_model: DataModel = None

    def __init__(self, client):
        self.client = client

    @property
    def api_endpoint(self) -> str:
        return f"{self.client.api_endpoint}/{self.object_type}"

    @property
    def export_endpoint(self) -> str:
        return f"{self.api_endpoint}/export/"

    @property
    def import_endpoint(self) -> str:
        return f"{self.api_endpoint}/import/"

    @property
    def info_endpoint(self) -> str:
        return f"{self.api_endpoint}/_info"

    def delete(self, ids: list[int]):
        return self.client.delete(
            self.api_endpoint,
            params={'q': json.dumps(ids)}
        )

    def put(self, obj_id: int, **request_body_kwargs):
        return self.client.put(
            url=f"{self.api_endpoint}/{obj_id}",
            json=json.dumps(request_body_kwargs)
        )

    def get_list(self, q: dict[str, any]) -> str:
        return self.client.get(
            self.api_endpoint,
            params={'q': json.dumps(q)}
        ).json()['result']

    def get(self, id: int) -> DataModel:
        res = self.client.get(
            f"{self.api_endpoint}/{id}",
        ).json()['result']
        return self.data_model.from_json(res, self)

    def find_all(self, page_size: int = 100, **kwargs) -> list[DataModel]:
        page: int = 0
        curr_l: list[DataModel] = self.find_by_page(page, page_size, **kwargs)
        objs: list[DataModel] = []
        while curr_l:
            objs.extend(curr_l)
            page += 1
            curr_l = self.find_by_page(page, page_size, **kwargs)
        return objs

    def find_by_name(self, name: str, page_size: int = 100) -> list[DataModel]:
        return self.find_all(page_size, **{self.data_model.name_field(): name})

    def find_by_page(self, page: int = 0, page_size: int = 100, **kwargs) -> list[DataModel]:
        query: dict[str, any] = dict(
            page=page,
            page_size=page_size,
            filters=[dict(col=k, opr='eq', value=v) for k, v in kwargs.items()]
        )
        return [self.data_model.from_json(obj, self) for obj in self.get_list(q=query)]

    def count(self) -> int:
        return self.client.get(
            f"{self.api_endpoint}",
        ).json()['count']

    def add(self, obj: DataModel) -> int:
        o = obj.to_json(columns=self.add_columns)
        response = self.client.post(self.api_endpoint, json=o)
        obj.id = response.json().get("id")
        obj.api_object = self
        return obj.id

    def export_to_file(self, ids: list[int], dir_path: str, filename: str) -> str:
        ids_array = ",".join([str(i) for i in ids])
        response = self.client.get(self.export_endpoint, params={"q": f"[{ids_array}]"})
        file_path: str = os.path.join(dir_path, filename)
        content_type = response.headers["content-type"].strip()
        if content_type.startswith("application/text"):
            data = yaml.load(response.text, Loader=yaml.FullLoader)
            file_path += ".yaml"
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False)
        elif content_type.startswith("application/json"):
            data = response.json()
            file_path += ".json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        elif content_type.startswith("application/zip"):
            data = response.content
            file_path += ".zip"
            with open(file_path, "wb") as f:
                f.write(data)
        else:
            raise ValueError(f"Unknown content type {content_type}")
        return file_path

    def export_to_buffer(self, ids: list[int]) -> io.BytesIO:
        ids_array = ",".join([str(i) for i in ids])
        response = self.client.get(self.export_endpoint, params={"q": f"[{ids_array}]"})
        content_type = response.headers["content-type"].strip()
        buffer = io.BytesIO()
        if content_type.startswith("application/text"):
            data = yaml.load(response.text, Loader=yaml.FullLoader)
            yaml.dump(data, buffer, default_flow_style=False)
        elif content_type.startswith("application/json"):
            data = response.json()
            json.dump(data, buffer, ensure_ascii=False, indent=4)
        elif content_type.startswith("application/zip"):
            buffer.write(response.content)
        else:
            raise ValueError(f"Unknown content type {content_type}")
        return buffer

    def import_from_buffer(self, buffer: io.BytesIO, overwrite: bool = False, passwords=None) -> requests.Response:
        passwords = {f"databases/{db}.yaml": pwd for db, pwd in (passwords or {}).items()}
        buffer.seek(0)
        files = dict(
            formData=("import_from_buffer.zip", buffer, f"application/zip"),
            passwords=(None, json.dumps(passwords), None),
        )
        response = self.client.post(
            self.import_endpoint,
            files=files,
            data=dict(overwrite=json.dumps(overwrite)),
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response

    def import_from_file(self, file_path: str, overwrite: bool = False, passwords=None) -> requests.Response:
        passwords = {f"databases/{db}.yaml": pwd for db, pwd in (passwords or {}).items()}
        file_ext = os.path.splitext(file_path)[-1].lstrip(".").lower()
        with open(file_path, "rb") as f:
            files = dict(
                formData=(file_path, f, f"application/{file_ext}"),
                passwords=(None, json.dumps(passwords), None),
            )
            response = self.client.post(
                self.import_endpoint,
                files=files,
                data=dict(overwrite=json.dumps(overwrite)),
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            return response


    @cached_property
    def _info(self) -> str:
        response = self.client.get(
            self.info_endpoint,
            params={"q": json.dumps({"keys": ["add_columns", "edit_columns"]})}
        )
        return response.json()

    @property
    def add_columns(self) -> list[str]:
        return [e.get("name") for e in self._info.get("add_columns", [])]

    @property
    def edit_columns(self) -> list[str]:
        return [e.get("name") for e in self._info.get("edit_columns", [])]
