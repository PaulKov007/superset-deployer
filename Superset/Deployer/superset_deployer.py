import logging
import re

import yaml
import os
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
from transliterate import translit
from Superset.SupersetApiClient.superset_client import SupersetClient
from SupersetApiClient.api_object import ApiObject
from SupersetApiClient.dashboards import Dashboard
from Deployer.extractors import SupersetObjectExtractor
from Deployer.importers import SupersetObjectImporter


class SupersetDeployer:
    DEPLOY_OBJ_NAME_REG_PATTERN: re.Pattern = re.compile(r"[^\w\d.,%()#_\[\]-]", re.IGNORECASE)

    def __init__(self, env: str, **config_kwargs):
        self.env = env
        with open(f'Deployer/{self.env}.yaml', 'r') as f:
            self.env_params: dict[str, str] = yaml.safe_load(f)
            self.api_client = SupersetClient(**self.env_params)
        with open('Deployer/config.yaml', 'r') as f:
            self.config: dict[str, any] = yaml.safe_load(f)
        for k in self.config:
            if config_kwargs.get(k):
                self.config[k] = config_kwargs[k]

        if 'deploy_path' not in self.config:
            raise Exception("You must define the deploy_path property in the config.yaml file")

        if not os.path.exists(self.config['deploy_path']):
            os.mkdir(self.config['deploy_path'])

        self.extractor = SupersetObjectExtractor(self)
        self.importer = SupersetObjectImporter(self)
        logging.getLogger().setLevel(logging.INFO)

    @staticmethod
    def get_deploy_object_name(src_object_name: str):
        obj_name: str = translit(src_object_name, 'ru', reversed=True).replace(' ', '_')
        return SupersetDeployer.DEPLOY_OBJ_NAME_REG_PATTERN.sub('', obj_name)

    def object_path(self, object_class: str) -> str:
        return os.path.join(self.config['deploy_path'], object_class)

    def get_deployed_object_names(self, object_class: str = 'datasets') -> list[str]:
        objects_dir: str = self.object_path(object_class)
        obj_names: list[str] = []
        for f in os.listdir(objects_dir):
            if f.endswith('.yaml'):
                with open(os.path.join(objects_dir, f), 'r', encoding='UTF-8') as f:
                    obj_yaml: dict[str, any] = yaml.safe_load(f)
                    api_object: ApiObject = getattr(self.api_client, object_class)
                    obj_name: str = obj_yaml[api_object.data_model.name_field()]
                    obj_names.append(obj_name)
        return obj_names

    def delete_empty_dashboards(self) -> None:
        untitled_dashboards: list[Dashboard] = self.api_client.dashboards.get_untitled_dashboards() or []
        del_ids: list[int] = []
        for dash in untitled_dashboards:
            if dt_parser.parse(dash.changed_on_utc).replace(tzinfo=None) \
                    < datetime.utcnow() - timedelta(days=self.config['empty_dash_retention_in_days']) \
                    and not dash.update().charts:
                del_ids.append(dash.id)
        self.api_client.dashboards.delete(ids=del_ids)

    def delete_duplicate_dashboards(self):
        dash_stat: dict[str, int] = dict()
        dash_l: list[Dashboard] = self.api_client.dashboards.find_all()
        titles: list[str] = [dash.dashboard_title for dash in dash_l]
        uniq_titles: set[str] = set(titles)

        for title in uniq_titles:
            dash_stat[title] = titles.count(title)

        for dash_title, cnt in dash_stat.items():
            if cnt > 1:
                sel_dash: Dashboard = max([d for d in dash_l if d.dashboard_title == dash_title],
                                          key=lambda d: d.changed_on_utc)
                dash_for_del: list[Dashboard] = [d for d in dash_l if d.dashboard_title == dash_title
                                                 and d.id != sel_dash.id]
                for dash in dash_for_del:
                    if dash.delete():
                        logging.info(f"Dashboard {dash.dashboard_title} ({dash.id}) was deleted")


if __name__ == '__main__':
    deployer = SupersetDeployer(env='dev')


    # deployer.extractor.extract_object('Top 10 Games: Proportion of Sales in Markets', 'charts')

    for dash in deployer.api_client.dashboards.find_all():
        print(f"Extract dashboard: {dash.dashboard_title}")
        deployer.extractor.extract_object(object_name=dash.dashboard_title, object_class='dashboards')

    deployer_prod = SupersetDeployer(env='prod')

    # objects_dir: str = deployer_prod.object_path(object_class)
    #
    # deployer_prod.importer.import_object('Slack Dashboard', object_class)

    object_class: str = 'dashboards'

    # for dash_title in deployer_prod.get_deployed_object_names(object_class):
    #     deployer_prod.importer.import_object(dash_title, object_class)
    #     print(dash_title)

    # for f in os.listdir(objects_dir):
    #     if f.endswith('.yaml'):
    #         with open(os.path.join(objects_dir, f)) as f:
    #             obj_yaml: dict[str, any] = yaml.safe_load(f)
    #             api_object: ApiObject = getattr(deployer_prod.api_client, object_class)
    #             obj_name: str = obj_yaml[api_object.data_model.name_field()]
    #             print(f'Import {obj_name}')
    #

    #
    # deployer_prod.build_object_for_import(object_name='Slack Dashboard', object_type='dashboards')
    # deployer.extract_object(object_name='New Members per Month', object_type='charts')
    # deployer.extract_object(object_name='members_channels_2', object_type='datasets')
    # deployer.build_object_for_import(object_name='New Members per Month', object_type='charts')

    # deployer_prod: Deployer = Deployer(env='prod')
    #

    # chart: Chart = deployer.api_client.charts.find_by_name('New Members per Month')[0]

    # deployer.build_object_for_import(object_name='New Members per Month', object_type='charts')

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

    # print(Dashboard.field_names())
    # print(Dataset.name_field())
    # print(deployer.get_charts())
