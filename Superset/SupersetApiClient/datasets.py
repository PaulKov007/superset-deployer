from dataclasses import dataclass, field
from SupersetApiClient.api_object import ApiObject
from SupersetApiClient.data_model import DataModel


@dataclass
class Dataset(DataModel):
    database_id: int = field(metadata=dict(json_parent="database", json_prop="id"))
    database_name: str = field(metadata=dict(json_parent="database", json_prop="database_name"))
    table_name: str = field(metadata=dict(is_name_field=True), default="")
    schema: str = ""
    columns: list = field(default_factory=list)
    description: str = ""
    kind: str = ""
    sql: str = ""


class Datasets(ApiObject):
    object_type = "dataset"
    data_model = Dataset
