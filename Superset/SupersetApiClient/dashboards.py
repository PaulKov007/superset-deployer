from deployer.superset.superset_api.api_object import ApiObject
from deployer.superset.superset_api.charts import Chart
from deployer.superset.superset_api.data_model import DataModel, default_string, json_field
from dataclasses import dataclass, field


@dataclass
class Dashboard(DataModel):
    dashboard_title: str = field(metadata=dict(is_name_field=True))
    changed_on_utc: str
    is_managed_externally: bool
    published: bool
    slug: str = field(default=None)
    json_metadata: dict = json_field()
    position_json: dict = json_field()
    changed_by: str = default_string()
    changed_by_name: str = default_string()
    changed_by_url: str = default_string()
    css: str = default_string()
    certified_by: str = default_string()
    changed_on: str = default_string()
    charts: list[str] = field(default_factory=list)

    @property
    def expanded_slices(self) -> dict:
        return self.json_metadata.get("expanded_slices", {})

    @expanded_slices.setter
    def expanded_slices(self, value: dict) -> None:
        self.json_metadata["expanded_slices"] = value

    def update_expanded_slices(self, value: dict) -> None:
        expanded_slices = self.expanded_slices
        expanded_slices.update(value)
        self.expanded_slices = expanded_slices

    def get_charts(self) -> list[Chart]:
        charts: list[Chart] = []
        for dash_chart in self.api_object.get_charts(self.id):
            charts.append(self.api_object.client.charts.get(dash_chart['id']))
        return charts

    def turn_chart_description(self):
        if self.json_metadata:
            expanded_slices: dict[str, bool] = {
                dash_slice["id"]: True
                for dash_slice in self.api_object.charts(self.id)
            }
            if expanded_slices:
                self.update_expanded_slices(expanded_slices)
                self.save()


class Dashboards(ApiObject):
    object_type = "dashboard"
    data_model = Dashboard

    @property
    def add_columns(self) -> list[str]:
        return [*set(super().add_columns + ['is_managed_externally'])]

    @property
    def edit_columns(self) -> list[str]:
        return [*set(super().edit_columns + ['is_managed_externally'])]

    def get_charts(self, dash_id_or_slug: str) -> str:
        return self.client.get(
            f"{self.api_endpoint}/{dash_id_or_slug}/charts"
        ).json()['result']

    def get_untitled_dashboards(self) -> list[Dashboard]:
        return self.find_by_name(name='[ untitled dashboard ]')

    def turn_chart_description(self) -> None:
        for dash in self.find_all():
            dash.turn_chart_description()
