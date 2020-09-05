from src.utils.spark_factory import get_spark
from src.utils.logger import Logger
from yaml import full_load
from os import path
import sys

logger = Logger()

APPLICATION_PATH = path.abspath(sys.modules['__main__'].__file__)[:-6]
config_file = APPLICATION_PATH + 'config.yaml'

with open(config_file) as file:
    config = full_load(file)

    SQL_PATH = config['sql_path']
    ODS_SCHEMA = config['ods_schema']

    PHOENIX_ZK_URL = config['phoenix_zk_url']
    EDS_SCHEMA = config['eds_schema']

    STAGING_SCHEMA = config['staging_schema']

    JDBC_TYPE = config['jdbc_type']
    JDBC_HOST = config['jdbc_host']
    JDBC_PORT = config['jdbc_port']
    JDBC_SCHEMA = config['jdbc_schema']
    JDBC_INSTANCE = config['jdbc_instance']
    JDBC_SID = config['jdbc_sid']
    JDBC_USER = config['jdbc_user']
    JDBC_PASSWORD = config['jdbc_password']