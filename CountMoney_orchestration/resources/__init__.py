import os
from CountMoney_orchestration.resources.pandas_io_manager import (
    pandas_sql_append_io_manager,
    pandas_sql_replace_io_manager,
)

TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
WAREHOUSE_HOSTS = os.getenv('WAREHOUSE_HOSTS')
WAREHOUSE_USER = os.getenv('WAREHOUSE_USER')
WAREHOUSE_SECRET = os.getenv('WAREHOUSE_SECRET')


tushare_append_io_manager = pandas_sql_append_io_manager.configured(
    {
        "warehouse_hosts": os.environ["WAREHOUSE_HOSTS"],
        "warehouse_user": os.environ["WAREHOUSE_USER"],
        "warehouse_secret": os.environ["WAREHOUSE_SECRET"],
        "destination_db": "datasources",
        "destination_schema": "tushare",
    }
)

tushare_replace_io_manager = pandas_sql_replace_io_manager.configured(
    {
        "warehouse_hosts": os.environ["WAREHOUSE_HOSTS"],
        "warehouse_user": os.environ["WAREHOUSE_USER"],
        "warehouse_secret": os.environ["WAREHOUSE_SECRET"],
        "destination_db": "datasources",
        "destination_schema": "tushare",
    }
)

resources_prod = {
    "tushare_pg_append_io_manager": tushare_append_io_manager,
    "tushare_pg_replace_io_manager": tushare_replace_io_manager,
}
