from dagster import job

##因为inputManager功能还在实验阶段，先用强撸的方式实现
from CountMoney_orchestration.resources import (
    WAREHOUSE_USER,
    WAREHOUSE_HOSTS,
    WAREHOUSE_SECRET,
)
from CountMoney_orchestration.ops.analysis import read_table

default_config = {
    "ops": {
        "read_table": {
            "config": {
                "hosts": WAREHOUSE_HOSTS,
                "user": WAREHOUSE_USER,
                "secret": WAREHOUSE_SECRET,
                "database": "warehouse",
                "schema": "finance",
                "table": "portfolio",
            }
        }
    }
}


@job(config=default_config)
def daily_push_job():
    read_table()
