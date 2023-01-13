import os
from CountMoney_orchestration.resources.pandas_io_manager import (
    pandas_sql_append_io_manager,
    pandas_sql_replace_io_manager,
)
from dagster import file_relative_path
from dagster_dbt import dbt_cli_resource

TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
WAREHOUSE_HOSTS = os.getenv('WAREHOUSE_HOSTS')
WAREHOUSE_USER = os.getenv('WAREHOUSE_USER')
WAREHOUSE_SECRET = os.getenv('WAREHOUSE_SECRET')
WECOM_BOT_TOKEN_KIKYO = os.getenv('WECOM_BOT_TOKEN_KIKYO')


tushare_append_io_manager = pandas_sql_append_io_manager.configured(
    {
        "warehouse_hosts": os.environ["WAREHOUSE_HOSTS"],
        "warehouse_user": os.environ["WAREHOUSE_USER"],
        "warehouse_secret": os.environ["WAREHOUSE_SECRET"],
        "destination_db": "warehouse",
        "destination_schema": "datasources",
    }
)

tushare_replace_io_manager = pandas_sql_replace_io_manager.configured(
    {
        "warehouse_hosts": os.environ["WAREHOUSE_HOSTS"],
        "warehouse_user": os.environ["WAREHOUSE_USER"],
        "warehouse_secret": os.environ["WAREHOUSE_SECRET"],
        "destination_db": "warehouse",
        "destination_schema": "datasources",
    }
)

# DBT相关配置
DBT_PROJECT_DIR = file_relative_path(__file__, "../../CountMoney_model")
DBT_PROFILES_DIR = file_relative_path(__file__, "../../CountMoney_model/config")
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": DBT_PROJECT_DIR,
        "profiles_dir": DBT_PROFILES_DIR,
    }
)

dbt_resource_def = {
    "dbt": dbt_resource,
}


resources_prod = {
    "tushare_pg_append_io_manager": tushare_append_io_manager,
    "tushare_pg_replace_io_manager": tushare_replace_io_manager,
    "dbt": dbt_resource,
}
