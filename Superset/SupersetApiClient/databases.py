from dataclasses import dataclass, field
from SupersetApiClient.api_object import ApiObject
from SupersetApiClient.data_model import DataModel, json_field, default_string


@dataclass
class Database(DataModel):
    database_name: str
    allow_ctas: bool = field(default=False)
    allow_cvas: bool = field(default=False)
    allow_dml: bool = field(default=False)
    allow_multi_schema_metadata_fetch: bool = field(default=False)
    allow_run_async: bool = field(default=False)
    expose_in_sqllab: bool = field(default=False)
    cache_timeout: int = field(default=None)
    encrypted_extra: str = default_string()
    engine: str = default_string()
    extra: dict = json_field()
    force_ctas_schema: str = default_string()
    server_cert: str = default_string()
    sqlalchemy_uri: str = default_string()


class Databases(ApiObject):
    object_type = "database"
    data_model = Database
