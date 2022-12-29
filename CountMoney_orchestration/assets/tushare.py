import tushare as ts
from dagster import asset
from CountMoney_orchestration.resources import TUSHARE_TOKEN


@asset(
    io_manager_key='tushare_pg_replace_io_manager',
    group_name="tushare",
    op_tags={"group": "EL"},
    name="stock_basic",
)
def stock_basic():
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('stock_basic', exchange='', list_status='L')
    return data


@asset(
    io_manager_key='tushare_pg_append_io_manager',
    group_name="tushare",
    op_tags={"group": "EL"},
    name="trade_calendar",
)
def trade_calendar():
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('trade_cal', start_date='20180101', end_date='20221231')
    return data
