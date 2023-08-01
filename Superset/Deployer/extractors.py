from __future__ import annotations
import io
import os
import zipfile
import logging
import yaml
from bidict import bidict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Deployer.superset_deployer import SupersetDeployer


class SupersetObjectExtractor:

    def __init__(self, deployer: SupersetDeployer):
        self.deployer = deployer
        self._reset_uuid_map()

    def _reset_uuid_map(self):
        self.uuid_map = {'datasets': bidict(), 'charts': bidict(),
                         'dashboards': bidict(), 'databases': bidict()
                         }

    @staticmethod
    def _get_files_from_zip(exported_zip: zipfile) -> dict[str, any]:
        files = {'datasets': {}, 'charts': {}, 'dashboards': {}, 'databases': {}}
        for f in exported_zip.filelist:
            name_parts: list[str] = f.filename.split("/")
            if name_parts[1] == 'datasets':
                database = name_parts[2]
                if database not in files['datasets']:
                    files['datasets'][database] = {}
                files['datasets'][database][name_parts[3]] = f.filename
            if name_parts[1] in ['charts', 'dashboards', 'databases']:
                files[name_parts[1]][name_parts[2]] = f.filename
        return files

    def extract_object(self,
                       object_name: str,
                       object_class: str,
                       only_build_uuid_map: bool = False
                       ) -> None:
        self._reset_uuid_map()
        objects: list = getattr(self.deployer.api_client, object_class).find_by_name(name=object_name)
        if objects:
            obj = max(objects, key=lambda el: el.id)
        else:
            if only_build_uuid_map:
                return
            raise Exception(f"{object_class} object named {object_name} not found")
        zip_buffer: io.BytesIO = obj.export_to_buffer()
        with zipfile.ZipFile(zip_buffer, 'r') as obj_zip:
            files: dict[str, any] = self._get_files_from_zip(obj_zip)
            obj_zip_extractor: ObjectZipExtractor = ObjectZipExtractor(self, obj_zip, only_build_uuid_map)
            # databases processing
            for db_filename, db_path in files['databases'].items():
                obj_zip_extractor.extract_database(db_path)
            # datasets processing
            for db_name, ds_dict in files['datasets'].items():
                for ds_filename, ds_path in ds_dict.items():
                    obj_zip_extractor.extract_dataset(ds_path)
            # charts processing
            for chart_filename, chart_path in files['charts'].items():
                obj_zip_extractor.extract_chart(chart_path)
            # dashboards processing
            for dash_filename, dash_path in files['dashboards'].items():
                obj_zip_extractor.extract_dashboard(dash_path)


class ObjectZipExtractor:
    def __init__(self,
                 ss_extractor: SupersetObjectExtractor,
                 object_zip: zipfile.ZipFile,
                 only_build_uuid_map: bool = False
                 ):
        self.ss_extractor = ss_extractor
        self.object_zip = object_zip
        self.only_build_uuid_map = only_build_uuid_map
        self.deployer = self.ss_extractor.deployer

    def extract_database(self, file_path: str) -> None:
        extract_dir: str = self.deployer.object_path('databases')
        if not self.only_build_uuid_map and not os.path.exists(extract_dir):
            os.mkdir(extract_dir)
            logging.info(f"Extract database. Directory {extract_dir} has been created")

        with self.object_zip.open(file_path, 'r') as db_file:
            db_yaml: dict[str, any] = yaml.safe_load(db_file)
            db_name: str = self.deployer.get_deploy_object_name(db_yaml['database_name'])
            self.ss_extractor.uuid_map['databases'][db_yaml['uuid']] = db_name
            if not self.only_build_uuid_map:
                with open(os.path.join(extract_dir, f"{db_name}.yaml"), 'wb') as db_new_file:
                    db_new_file.write(yaml.dump(db_yaml, allow_unicode=True, encoding='UTF-8'))

    def extract_dataset(self, file_path: str) -> None:
        extract_dir: str = self.deployer.object_path('datasets')
        if not self.only_build_uuid_map and not os.path.exists(extract_dir):
            os.mkdir(extract_dir)
            logging.info(f"Extract dataset. Directory {extract_dir} has been created")

        with self.object_zip.open(file_path, 'r') as ds_file:
            ds_yaml: dict[str, any] = yaml.safe_load(ds_file)
            ds_name: str = self.deployer.get_deploy_object_name(ds_yaml['table_name'])
            self.ss_extractor.uuid_map['datasets'][ds_yaml['uuid']] = ds_name
            if not self.only_build_uuid_map:
                if ds_yaml.get('sql'):
                    sql_file_name: str = f"{ds_name}.sql"
                    with open(os.path.join(extract_dir, sql_file_name), 'w', encoding='UTF-8') as ds_sql:
                        ds_sql.write(ds_yaml['sql'])
                    ds_yaml['sql'] = f'#file:{sql_file_name}#'
                with open(os.path.join(extract_dir, f"{ds_name}.yaml"), 'wb') as ds_new_file:
                    ds_yaml['_deploy_database_name'] = self.ss_extractor.uuid_map['databases'][ds_yaml['database_uuid']]
                    ds_new_file.write(yaml.dump(ds_yaml, allow_unicode=True, encoding='UTF-8'))

    def extract_chart(self, file_path: str) -> None:
        extract_dir: str = self.deployer.object_path('charts')
        if not self.only_build_uuid_map and not os.path.exists(extract_dir):
            os.mkdir(extract_dir)
            logging.info(f"Extract chart. Directory {extract_dir} has been created")

        with self.object_zip.open(file_path, 'r') as chart_file:
            chart_yaml: dict[str, any] = yaml.safe_load(chart_file)

            if 'dataset_uuid' not in chart_yaml:
                logging.warning(f"Chart {chart_yaml['slice_name']} without dataset, skipping")
                return None

            chart_name: str = self.deployer.get_deploy_object_name(chart_yaml['slice_name'])
            self.ss_extractor.uuid_map['charts'][chart_yaml['uuid']] = chart_name
            if not self.only_build_uuid_map:
                with open(os.path.join(extract_dir, f"{chart_name}.yaml"), 'wb') as chart_new_file:
                    chart_yaml['_deploy_dataset_name'] = self.ss_extractor.uuid_map['datasets'][
                        chart_yaml['dataset_uuid']]
                    chart_new_file.write(yaml.dump(chart_yaml, allow_unicode=True, encoding='UTF-8'))

    def extract_dashboard(self, file_path: str) -> None:
        extract_dir: str = self.deployer.object_path('dashboards')
        if not self.only_build_uuid_map and not os.path.exists(extract_dir):
            os.mkdir(extract_dir)
            logging.info(f"Extract dashboard. Directory {extract_dir} has been created")

        with self.object_zip.open(file_path, 'r') as dash_file:
            dash_yaml: dict[str, any] = yaml.safe_load(dash_file)
            dash_name: str = self.deployer.get_deploy_object_name(dash_yaml['dashboard_title'])
            self.ss_extractor.uuid_map['dashboards'][dash_yaml['uuid']] = dash_name
            if not self.only_build_uuid_map:
                if 'metadata' in dash_yaml and 'native_filter_configuration' in dash_yaml['metadata']:
                    for dash_filter in dash_yaml['metadata']['native_filter_configuration']:
                        if 'targets' in dash_filter:
                            for target in dash_filter['targets']:
                                if 'datasetUuid' in target:
                                    if target['datasetUuid'] not in self.ss_extractor.uuid_map['datasets']:
                                        logging.warning(
                                            f"Dashboard {dash_name} has incorrect link to dataset, skipping"
                                        )
                                        continue
                                    target['_deploy_dataset_name'] = self.ss_extractor.uuid_map['datasets'][
                                        target['datasetUuid']]

                charts_to_delete: list[str] = []

                for pos_key, pos_val in dash_yaml['position'].items():
                    if type(pos_val) is dict and pos_key.lower().startswith('chart'):
                        chart_uuid: str = pos_val['meta'].get('uuid')
                        if chart_uuid not in self.ss_extractor.uuid_map['charts']:
                            charts_to_delete.append(pos_key)
                            continue
                        pos_val['meta']['_deploy_chart_name'] = \
                            self.ss_extractor.uuid_map['charts'][pos_val['meta']['uuid']]

                for chart_key in charts_to_delete:
                    dash_yaml['position'].pop(chart_key)

                if 'slug' in dash_yaml and not dash_yaml['slug']:
                    dash_yaml['slug'] = dash_name

                with open(os.path.join(extract_dir, f"{dash_name}.yaml"), 'wb') as dash_new_file:
                    dash_new_file.write(yaml.dump(dash_yaml, allow_unicode=True, encoding='UTF-8'))
