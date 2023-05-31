import psycopg2
import re
import yaml
from typing import Dict, List
import os


class SupersetRolesDeployer:

    def __init__(self, env: str):
        self.env = env
        conn_info = {
            'host': creds_superset.get("host"),
            'port': creds_superset.get("port"),
            'user': creds_superset.get("user"),
            'password': creds_superset.get("password"),
            'database': creds_superset.get("database")
        }
