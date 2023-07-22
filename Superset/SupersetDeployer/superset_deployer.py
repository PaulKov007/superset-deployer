import argparse
import zipfile
from dataclasses import fields
import io

import yaml
import os
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
import json

from transliterate import translit

from Superset.SupersetApiClient.superset_client import SupersetClient
from SupersetApiClient.dashboards import Dashboard
from SupersetApiClient.datasets import Dataset


class SupersetDeployer:

    def __init__(self, env: str, **config_kwargs):
        self.api_client = SupersetClient(
            api_endpoint="http://192.168.60.128:8088/api/v1",
            username="admin",
            password="admin"
        )
        with open('SupersetDeployer/config.yaml', 'r') as f:
            self.config: dict[str, any] = yaml.safe_load(f)
        for k in self.config:
            if config_kwargs.get(k):
                self.config[k] = config_kwargs[k]

    def object_path(self, object_type: str) -> str:
        return os.path.join(self.config['deploy_path'], object_type)

    def delete_empty_dashboards(self):
        untitled_dashboards: list[Dashboard] = self.api_client.dashboards.get_untitled_dashboards() or []
        del_ids: list[int] = []
        for dash in untitled_dashboards:
            if dt_parser.parse(dash.changed_on_utc).replace(tzinfo=None) \
                    < datetime.utcnow() - timedelta(days=self.config['empty_dash_retention_in_days']) \
                    and not dash.update().charts:
                del_ids.append(dash.id)
        self.api_client.dashboards.delete(ids=del_ids)

    def extract_dataset(self, dataset_name: str):
        dataset_path: str = self.object_path('datasets')
        datasets: list[Dataset] = self.api_client.datasets.find_by_name(name=dataset_name)
        if datasets:
            ds: Dataset = datasets[0]
        else:
            raise Exception(f"Dataset {dataset_name} not found")
        zip_buffer: io.BytesIO = ds.export_to_buffer()
        with zipfile.ZipFile(zip_buffer, 'r') as ds_zip:
            for f in ds_zip.filelist:
                src_file_name: str = f.filename
                name_parts: list[str] = os.path.splitext(src_file_name)[0].split("/")[1::]
                object_name: str = translit(name_parts[-1], 'ru', reversed=True).replace(' ', '_')
                if 'databases' in name_parts:
                    ds_zip.getinfo(f.filename).filename = f"{object_name}.yaml"
                    ds_zip.extract(src_file_name, path=self.object_path('databases'))
                if 'datasets' in name_parts:
                    db_name: str = name_parts[1]
                    dataset_name: str = f'{db_name}.{object_name}'
                    with ds_zip.open(src_file_name, 'r') as ds_file:
                        ds_yaml: dict[str, any] = yaml.safe_load(ds_file)
                        sql_file_name: str = f"{dataset_name}.sql"
                        with open(os.path.join(dataset_path, 'sql', sql_file_name), 'w', encoding='UTF-8') as ds_sql:
                            ds_sql.write(ds_yaml['sql'])
                        with open(os.path.join(dataset_path, f"{dataset_name}.yaml"), 'wb') as ds_new_file:
                            ds_yaml['sql'] = f'#file:{sql_file_name}#'
                            ds_yaml['_deploy_database_name'] = f'#database:{db_name}.yaml#'
                            ds_new_file.write(yaml.dump(ds_yaml, allow_unicode=True, encoding='UTF-8'))

    def build_dataset_for_import(self, dataset_name: str):






        # ds_file: zipfile.ZipFile = zipfile.ZipFile.open()

        # read zip
        # normalize (extract dataset yaml/sql, extract database)


if __name__ == '__main__':
    deployer = SupersetDeployer(env='prod')

    deployer.extract_dataset(dataset_name='test_dataset')

    # dashboards: list[Dashboard] = deployer.api_client.dashboards.find_by_name("USA Births Names")
    #
    # dashboards[0].update()
    # print(dashboards[0])
    # print(dashboards[0].charts)
    # print([sl.slice_name for sl in dashboards[0].get_charts()])

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

    # print([dash.update() for dash in deployer.api_client.dashboards.find_all()])


    # print(deployer.delete_empty_dashboards())

    # deployer.turn_chart_description()

    # s: str = '{"timed_refresh_immune_slices": [], "expanded_slices": {}, "refresh_frequency": 0, "default_filters": "{\"176\": {\"__time_range\": \"No filter\"}}", "color_scheme": "supersetColors", "label_colors": {"Medium": "#1FA8C9", "Small": "#454E7C", "Large": "#5AC189", "SUM(SALES)": "#1FA8C9", "Classic Cars": "#454E7C", "Vintage Cars": "#5AC189", "Motorcycles": "#FF7F44", "Trucks and Buses": "#666666", "Planes": "#E04355", "Ships": "#FCC700", "Trains": "#A868B7"}, "chart_configuration": {}, "color_scheme_domain": ["#1FA8C9", "#454E7C", "#5AC189", "#FF7F44", "#666666", "#E04355", "#FCC700", "#A868B7", "#3CCCCB", "#A38F79", "#8FD3E4", "#A1A6BD", "#ACE1C4", "#FEC0A1", "#B2B2B2", "#EFA1AA", "#FDE380", "#D3B3DA", "#9EE5E5", "#D1C6BC"], "shared_label_colors": {}, "cross_filters_enabled": true}'

    # print(s)
    # pyperclip.copy(s)

    # deployer.build_dataset()

    #print(Dashboard.field_names())
    #print(Dataset.name_field())
    # print(deployer.get_charts())




