import dataclasses
import datetime
import json
import os
from SupersetApiClient.exceptions import *


def json_field():
    return dataclasses.field(default=None, repr=False)


def default_string():
    return dataclasses.field(default="", repr=False)


@dataclasses.dataclass
class DataModel:
    id: int
    api_object = None

    @classmethod
    def fields(cls) -> tuple[dataclasses.Field]:
        return dataclasses.fields(cls)

    @classmethod
    def name_field(cls) -> str:
        res_l: list = [f.name for f in cls.fields() if f.metadata.get('is_name_field')]
        if res_l:
            if len(res_l) > 1:
                raise MultipleNameFieldFound()
            else:
                return res_l[0]
        else:
            raise NameFieldNotFound()

    def get_name(self) -> str:
        return getattr(self, self.name_field())

    @classmethod
    def field_names(cls) -> set[str]:
        return {f.name for f in cls.fields()}

    @classmethod
    def json_field_names(cls) -> set[str]:
        return {f.name for f in cls.fields() if f.type is dict}

    def __post_init__(self):
        for f in self.json_field_names():
            setattr(self, f, json.loads(getattr(self, f) or "{}"))

    @classmethod
    def from_json(cls, src_json: dict, api_object):
        res_dict: dict[str, any] = dict()
        for f in cls.fields():
            el: any = src_json.get(f.name)
            if f.metadata.get("json_parent") in src_json:
                el = src_json[f.metadata["json_parent"]][f.metadata["json_prop"]]
            if el is not None:
                res_dict[f.name] = el
        cls.api_object = api_object
        return cls(**res_dict)

    def to_json(self, columns: list[str] = None) -> dict:
        res_json = {}
        iter_fields: tuple[dataclasses.Field]
        if columns:
            iter_fields = (f for f in self.fields()
                           if f.name in columns or f.metadata.get("json_parent") in columns)
        else:
            iter_fields = self.fields()
        for f in iter_fields:
            if not hasattr(self, f.name):
                continue
            value = getattr(self, f.name)
            if f.type is dict:
                value = json.dumps(value)
            if f.metadata.get("json_parent"):
                meta_dict: dict[str, any] = res_json.get(f.metadata["json_parent"]) or {}
                meta_dict.update({f.metadata["json_prop"]: value})
                res_json[f.metadata["json_parent"]] = meta_dict
            else:
                res_json[f.name] = value
        return res_json

    @property
    def api_endpoint(self) -> str:
        return f"{self.api_object.api_endpoint}/{self.id}"

    def export_name(self, add_ts: bool = False) -> str:
        return f"{self.api_object.object_type}_{self.get_name()}" \
               f"{datetime.datetime.now().strftime('_%Y%m%dT%H%M%S') if add_ts else ''}"

    def export(self, dir_path: str, filename: str = None) -> str:
        return self.api_object.export(ids=[self.id], dir_path=dir_path, filename=filename or self.export_name())

    def update(self) -> None:
        res = self.api_object.client.get(self.api_endpoint).json().get("result")
        for f in self.fields():
            if f.name in res or f.metadata.get("json_parent") in res:
                f_name: str = f.metadata.get("json_parent") or f.name
                json_prop: str = f.metadata.get("json_prop")
                if f.type is dict:
                    setattr(self, f.name, json.loads(res[f_name] or "{}"))
                else:
                    setattr(self, f.name,
                            res[f_name][json_prop] if type(res[f_name]) is dict and json_prop else res[f_name])

    def save(self) -> None:
        json_obj = self.to_json(columns=self.api_object.edit_columns)
        self.api_object.client.put(self.api_endpoint, json=json_obj)

    def delete(self) -> bool:
        return self.api_object.client.delete(self.api_endpoint).json().get("message") == "OK"







