import datetime

import pandas as pd
import tushare as ts
from dagster import asset
from CountMoney_orchestration.resources import TUSHARE_TOKEN
from datetime import date
from time import time
from CountMoney_orchestration.utils import date_trans


@asset(
    io_manager_key="tushare_pg_replace_io_manager",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="stock_basic",
)
def stock_basic():
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('stock_basic', exchange='', list_status='L')
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="trade_calendar",
)
def trade_calendar():
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('trade_cal', start_date='20180101', end_date='20221231')
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="balance_sheet",
    config_schema={"mode": str},
)
def balance_sheet(context) -> pd.DataFrame:
    """
    资产负债表
    :param context:
    :return:
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    if context.op_config["mode"] == "history":
        report_period = date_trans.report_date_generate(
            ["2020", "2021", "2022"]
        )  ##回溯3年历史数据
        data = pro.balancesheet_vip(period=report_period[0], report_type=1)
        data.drop(data.index, inplace=True)
        for i in range(len(report_period)):
            data_q = pro.balancesheet_vip(period=report_period[i], report_type=1)
            data = pd.concat([data, data_q])
    elif context.op_config["mode"] == "daily":
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.balancesheet_vip(ann_date=today, report_type=1)
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))
    created_at = round(time())
    data['created_at'] = created_at
    return data
