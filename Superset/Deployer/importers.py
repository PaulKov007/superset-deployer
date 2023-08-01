from __future__ import annotations
import os
import zipfile
from datetime import datetime
from typing import Union
import yaml
from SupersetApiClient.api_object import ApiObject
from typing import TYPE_CHECKING

from SupersetApiClient.dashboards import Dashboard

if TYPE_CHECKING:
    from Deployer.superset_deployer import SupersetDeployer


class SupersetObjectImporter:
    SS_OBJECTS_MAP: dict[str, dict[str, str]] = {
        'databases': dict(ss_type='Database', child_object_class=None),
        'datasets': dict(ss_type='SqlaTable', child_object_class='databases'),
        'charts': dict(ss_type='Slice', child_object_class='datasets'),
        'dashboards': dict(ss_type='Dashboard', child_object_class='charts'),
    }

    def __init__(self, deployer: SupersetDeployer):
        self.deployer = deployer

    def _get_object_map(self, object_class: str) -> dict[str, str]:
        dict_res: dict[str, str] = {object_class: SupersetObjectImporter.SS_OBJECTS_MAP[object_class]['ss_type']}
        if object_class == self.deployer.config.get('min_level_deployment_object_class'):
            return dict_res
        child_obj_class: str = SupersetObjectImporter.SS_OBJECTS_MAP[object_class].get('child_object_class')

        while child_obj_class:
            dict_res = {child_obj_class: SupersetObjectImporter.SS_OBJECTS_MAP[child_obj_class]['ss_type'],
                        **dict_res}
            if child_obj_class == self.deployer.config.get('min_level_deployment_object_class'):
                return dict_res
            child_obj_class = SupersetObjectImporter.SS_OBJECTS_MAP[child_obj_class].get('child_object_class')
        return dict_res

    @staticmethod
    def _update_zip_file(zip_path: str, filename: str, data: Union[str, bytes]) -> None:
        zip_dirname: str = os.path.dirname(zip_path)
        zip_filename: str = os.path.splitext(zip_path)[0].split(os.path.sep)[-1]
        tmp_zip_path: str = os.path.join(zip_dirname, f"{zip_filename}_tmp.zip")
        with zipfile.ZipFile(zip_path, 'r') as zin:
            with zipfile.ZipFile(tmp_zip_path, 'w') as zout:
                zout.comment = zin.comment
                for item in zin.infolist():
                    if item.filename != filename:
                        zout.writestr(item, zin.read(item.filename))
        os.remove(zip_path)
        os.rename(tmp_zip_path, zip_path)
        with zipfile.ZipFile(zip_path, 'a') as zf:
            zf.writestr(filename, data)

    def import_object(self,
                      object_name: str,
                      object_class: str = 'datasets'
                      ) -> None:
        # build uuids map
        self.deployer.extractor.extract_object(object_name, object_class, True)

        zip_filename: str = f"import_{self.deployer.env}_{object_class}_{object_name}.zip"
        zip_dirname: str = os.path.splitext(zip_filename)[0]
        obj_deploy_name: str = self.deployer.get_deploy_object_name(object_name)
        metadata: dict[str, any] = {
            'version': '1.0.0',
            'timestamp': datetime.now().isoformat()
        }
        with zipfile.ZipFile(zip_filename, 'w') as obj_zip:
            import_builder: SupersetObjectImportBuilder = SupersetObjectImportBuilder(
                self,
                obj_zip,
                zip_dirname
            )
            getattr(import_builder, f"build_{object_class[:-1]}_for_import")(obj_deploy_name)

        for build_obj_class, ss_obj_type in self._get_object_map(object_class).items():
            metadata['type'] = ss_obj_type
            self._update_zip_file(zip_filename, f"{zip_dirname}/metadata.yaml", yaml.dump(metadata))
            api_obj: ApiObject = getattr(self.deployer.api_client, build_obj_class)
            passwords: dict[str, str] = {
                "examples": "superset"
            }
            api_obj.import_from_file(zip_filename, True, passwords)

        if object_class == 'dashboards':
            dashboards: list = self.deployer.api_client.dashboards.find_by_name(name=object_name)
            if dashboards:
                dash: Dashboard = max(dashboards, key=lambda el: el.id)
                dash.published = True
                dash.save()


class SupersetObjectImportBuilder:
    def __init__(self,
                 ss_importer: SupersetObjectImporter,
                 object_zip: zipfile.ZipFile,
                 zip_root_dir: str
                 ):
        self.ss_importer = ss_importer
        self.object_zip = object_zip
        self.zip_root_dir = zip_root_dir
        self.objects_to_build: list[str] = list()
        self.deployer = self.ss_importer.deployer
        self.extractor = self.deployer.extractor

    def build_database_for_import(self, db_deploy_name: str) -> None:
        db_dir: str = self.deployer.object_path('databases')
        db_path: str = os.path.join(db_dir, f"{db_deploy_name}.yaml")

        if db_path in self.objects_to_build:
            return

        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database {db_deploy_name} not found in directory {db_dir}")

        with open(db_path, 'r', encoding='UTF-8') as db_file:
            db_yaml: dict[str, any] = yaml.safe_load(db_file)
            if self.extractor.uuid_map['databases'].inverse.get(db_deploy_name):
                db_yaml['uuid'] = self.extractor.uuid_map['databases'].inverse[db_deploy_name]
            zip_db_path: str = os.path.join(self.zip_root_dir, 'databases', f"{db_deploy_name}.yaml")
            self.object_zip.writestr(zip_db_path, yaml.dump(db_yaml))
        self.objects_to_build.append(db_path)

    def build_dataset_for_import(self, ds_deploy_name: str) -> None:
        ds_dir: str = self.deployer.object_path('datasets')
        ds_path: str = os.path.join(ds_dir, f"{ds_deploy_name}.yaml")

        if ds_path in self.objects_to_build:
            return

        if not os.path.exists(ds_path):
            raise FileNotFoundError(f"Dataset {ds_deploy_name} not found in directory {ds_dir}")

        with open(ds_path, 'r', encoding='UTF-8') as ds_file:
            ds_yaml: dict[str, any] = yaml.safe_load(ds_file)
            if '_deploy_database_name' not in ds_yaml:
                raise Exception(f"Property _deploy_database_name not found in {ds_path} file")
            if self.extractor.uuid_map['datasets'].inverse.get(ds_deploy_name):
                ds_yaml['uuid'] = self.extractor.uuid_map['datasets'].inverse[ds_deploy_name]
            sql_path: str = os.path.join(ds_dir, f"{ds_deploy_name}.sql")
            # read sql
            if os.path.exists(sql_path):
                with open(sql_path, 'r', encoding='UTF-8') as sql_file:
                    ds_yaml['sql'] = sql_file.read()
            db_name: str = ds_yaml['_deploy_database_name']
            if self.extractor.uuid_map['databases'].inverse.get(db_name):
                ds_yaml['database_uuid'] = self.extractor.uuid_map['databases'].inverse[db_name]
            self.build_database_for_import(db_name)
            del ds_yaml['_deploy_database_name']

            zip_ds_path: str = os.path.join(self.zip_root_dir, 'datasets', db_name, f"{ds_deploy_name}.yaml")
            self.object_zip.writestr(zip_ds_path, yaml.dump(ds_yaml))
        self.objects_to_build.append(ds_path)

    def build_chart_for_import(self, chart_deploy_name: str) -> None:
        chart_dir: str = self.deployer.object_path('charts')
        chart_path: str = os.path.join(chart_dir, f"{chart_deploy_name}.yaml")

        if not os.path.exists(chart_path):
            raise FileNotFoundError(f"Chart {chart_deploy_name} not found in directory {chart_dir}")

        with open(chart_path, 'r', encoding='UTF-8') as chart_file:
            chart_yaml: dict[str, any] = yaml.safe_load(chart_file)
            if '_deploy_dataset_name' not in chart_yaml:
                raise Exception(f"Property _deploy_dataset_name not found in {chart_path} file")
            if self.extractor.uuid_map['charts'].inverse.get(chart_deploy_name):
                chart_yaml['uuid'] = self.extractor.uuid_map['charts'].inverse[chart_deploy_name]
            ds_name: str = chart_yaml['_deploy_dataset_name']
            if self.extractor.uuid_map['datasets'].inverse.get(ds_name):
                chart_yaml['dataset_uuid'] = self.extractor.uuid_map['datasets'].inverse[ds_name]
            self.build_dataset_for_import(ds_name)
            del chart_yaml['_deploy_dataset_name']

            zip_chart_path: str = os.path.join(self.zip_root_dir, 'charts', f"{chart_deploy_name}.yaml")
            self.object_zip.writestr(zip_chart_path, yaml.dump(chart_yaml))

    def build_dashboard_for_import(self, dash_deploy_name: str) -> None:
        dashboard_dir: str = self.deployer.object_path('dashboards')
        dashboard_path: str = os.path.join(dashboard_dir, f"{dash_deploy_name}.yaml")

        if not os.path.exists(dashboard_path):
            raise FileNotFoundError(f"Dashboard {dashboard_path} not found in directory {dashboard_dir}")

        with open(dashboard_path, 'r', encoding='UTF-8') as dash_file:
            dash_yaml: dict[str, any] = yaml.safe_load(dash_file)
            if self.extractor.uuid_map['dashboards'].inverse.get(dash_deploy_name):
                dash_yaml['uuid'] = self.extractor.uuid_map['dashboards'].inverse[dash_deploy_name]
            if 'metadata' in dash_yaml and 'native_filter_configuration' in dash_yaml['metadata']:
                for dash_filter in dash_yaml['metadata']['native_filter_configuration']:
                    if 'targets' in dash_filter:
                        for target in dash_filter['targets']:
                            if target.get('_deploy_dataset_name'):
                                ds_name: str = target['_deploy_dataset_name']
                                if self.extractor.uuid_map['datasets'].inverse.get(ds_name):
                                    target['datasetUuid'] = self.extractor.uuid_map['datasets'].inverse[ds_name]
                                self.build_dataset_for_import(ds_name)
                                del target['_deploy_dataset_name']

            for pos_key, pos_val in dash_yaml['position'].items():
                if type(pos_val) is dict and pos_key.lower().startswith('chart') \
                        and 'uuid' in pos_val.get('meta') \
                        and '_deploy_chart_name' in pos_val.get('meta'):
                    chart_name: str = pos_val['meta']['_deploy_chart_name']
                    if self.extractor.uuid_map['charts'].inverse.get(chart_name):
                        pos_val['meta']['uuid'] = self.extractor.uuid_map['charts'].inverse[chart_name]
                    self.build_chart_for_import(chart_name)
                    del pos_val['meta']['_deploy_chart_name']

            zip_dash_path: str = os.path.join(self.zip_root_dir, 'dashboards', f"{dash_deploy_name}.yaml")
            self.object_zip.writestr(zip_dash_path, yaml.dump(dash_yaml))
