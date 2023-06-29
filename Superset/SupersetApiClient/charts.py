from SupersetApiClient.api_object import ApiObject
from SupersetApiClient.data_model import DataModel, default_string, json_field
from dataclasses import dataclass, field


@dataclass
class Chart(DataModel):
    description: str = default_string()
    slice_name: str = field(metadata=dict(is_name_field=True), default="")
    params: dict = json_field()
    datasource_id: int = None
    datasource_type: str = default_string()
    viz_type: str = ""
    dashboards: list[int] = field(default_factory=list)


class Charts(ApiObject):
    object_type = "chart"
    data_model = Chart

    @property
    def add_columns(self):
        return [
            "datasource_id",
            "datasource_type",
            "slice_name",
            "params",
            "viz_type",
            "description",
        ]

