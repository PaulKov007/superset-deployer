import argparse
from dataclasses import fields

import yaml
import os
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
import json
from Superset.SupersetApiClient.superset_client import SupersetClient
from SupersetApiClient.dashboards import Dashboard
from SupersetApiClient.data_model import DataModel
from SupersetApiClient.datasets import Dataset


class SupersetDeployer:

    def __init__(self, deploy_path: str):
        self.api_client = SupersetClient(
            api_endpoint="http://185.130.115.142:8088/api/v1",
            username="admin",
            password="admin"
        )
        self.deploy_path = deploy_path

    def delete_empty_dashboards(self):
        untitled_dashboards: list[Dashboard] = self.api_client.dashboards.get_untitled_dashboards() or []
        for dash in untitled_dashboards:
            dash.update()
            if dt_parser.parse(dash.changed_on_utc).replace(tzinfo=None) \
                    < datetime.utcnow() - timedelta(days=2) and not dash.charts:
                dash.delete()

    def deploy_dataset(self, dataset_name: str):
        dataset_path: str = os.path.join(self.deploy_path, 'datasets')


if __name__ == '__main__':
    deployer = SupersetDeployer(deploy_path="BI/superset")

    # ds: Dataset = deployer.api_client.datasets.find_by_name("test dataset")[0]
    # # print(ds)
    # # ds.update()
    # # print(ds)
    #
    # print(ds.export_name(True))
    # print(ds.export(""))

    # dashboards: list[Dashboard] = deployer.api_client.dashboards.find_by_name("USA Births Names")
    # dashboards[0].update()
    # print(dashboards[0])

    deployer.api_client.dashboards.turn_chart_description()


    # print(deployer.delete_empty_dashboards())

    # deployer.turn_chart_description()

    # s: str = '{"timed_refresh_immune_slices": [], "expanded_slices": {}, "refresh_frequency": 0, "default_filters": "{\"176\": {\"__time_range\": \"No filter\"}}", "color_scheme": "supersetColors", "label_colors": {"Medium": "#1FA8C9", "Small": "#454E7C", "Large": "#5AC189", "SUM(SALES)": "#1FA8C9", "Classic Cars": "#454E7C", "Vintage Cars": "#5AC189", "Motorcycles": "#FF7F44", "Trucks and Buses": "#666666", "Planes": "#E04355", "Ships": "#FCC700", "Trains": "#A868B7"}, "chart_configuration": {}, "color_scheme_domain": ["#1FA8C9", "#454E7C", "#5AC189", "#FF7F44", "#666666", "#E04355", "#FCC700", "#A868B7", "#3CCCCB", "#A38F79", "#8FD3E4", "#A1A6BD", "#ACE1C4", "#FEC0A1", "#B2B2B2", "#EFA1AA", "#FDE380", "#D3B3DA", "#9EE5E5", "#D1C6BC"], "shared_label_colors": {}, "cross_filters_enabled": true}'

    # print(s)
    # pyperclip.copy(s)

    # deployer.build_dataset()

    #print(Dashboard.field_names())
    #print(Dataset.name_field())
    # print(deployer.get_charts())




