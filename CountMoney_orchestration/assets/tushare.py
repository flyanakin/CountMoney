import datetime

import pandas as pd
import tushare as ts
from dagster import asset
from CountMoney_orchestration.resources import TUSHARE_TOKEN
from datetime import date
from time import time
import hashlib
from CountMoney_orchestration.utils import date_trans


@asset(
    io_manager_key="tushare_pg_replace_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_stock_basic",
)
def tushare_stock_basic() -> pd.DataFrame:
    """
    tushare股票列表
    仅获取上市公司，不含退市和暂停上市
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=25
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('stock_basic', exchange='', list_status='L')
    return data


@asset(
    io_manager_key="tushare_pg_replace_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_trade_calendar",
)
def tushare_trade_calendar() -> pd.DataFrame:
    """
    股票交易日历
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=26
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.query('trade_cal', start_date='20180101', end_date='20221231')
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_balance_sheet",
    config_schema={
        "mode": str,
        "ts_code": str,
        "ann_date": str,
        "start_date": str,
        "end_date": str,
        "period": str,
    },
)
def tushare_balance_sheet(context) -> pd.DataFrame:
    """
    资产负债表
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=36
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __ann_date = context.op_config["ann_date"]
    __start_date = context.op_config["start_date"]
    __end_date = context.op_config["end_date"]
    __period = context.op_config["period"]

    if __mode == "history":
        report_period = date_trans.report_date_generate(
            ["2020", "2021", "2022"]
        )  ##回溯3年历史数据
        data = pro.balancesheet_vip(period=report_period[0], report_type=1)
        data.drop(data.index, inplace=True)
        for i in range(len(report_period)):
            data_q = pro.balancesheet_vip(period=report_period[i], report_type=1)
            data = pd.concat([data, data_q])
    elif __mode == "daily":
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.balancesheet_vip(ann_date=today, report_type=1)
    elif __mode == "para":
        data = pro.balancesheet_vip(
            ts_code=__ts_code,
            period=__period,
            ann_date=__ann_date,
            start_date=__start_date,
            end_date=__end_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    ##生成md5 id
    data['md5'] = (
        data['ts_code'] + data['f_ann_date'] + data['end_date'] + data['update_flag']
    )
    data['statement_id'] = [
        hashlib.md5(val.encode('utf-8')).hexdigest() for val in data['md5']
    ]
    data = data.drop(['md5'], axis=1)
    ##生成入库时间
    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_income_statement",
    config_schema={
        "mode": str,
        "ts_code": str,
        "ann_date": str,
        "start_date": str,
        "end_date": str,
        "period": str,
    },
)
def tushare_income_statement(context) -> pd.DataFrame:
    """
    利润表（单季）
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=33
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __ann_date = context.op_config["ann_date"]
    __start_date = context.op_config["start_date"]
    __end_date = context.op_config["end_date"]
    __period = context.op_config["period"]

    if __mode == "history":
        report_period = date_trans.report_date_generate(
            ["2020", "2021", "2022"]
        )  ##回溯3年历史数据
        data = pro.income_vip(period=report_period[0], report_type=2)
        data.drop(data.index, inplace=True)
        for i in range(len(report_period)):
            data_q = pro.income_vip(period=report_period[i], report_type=2)
            data = pd.concat([data, data_q])
    elif __mode == "daily":
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.income_vip(ann_date=today, report_type=2)
    elif __mode == "para":
        data = pro.income_vip(
            ts_code=__ts_code,
            period=__period,
            ann_date=__ann_date,
            start_date=__start_date,
            end_date=__end_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    ##生成md5 id
    data['md5'] = (
        data['ts_code'] + data['f_ann_date'] + data['end_date'] + data['update_flag']
    )
    data['statement_id'] = [
        hashlib.md5(val.encode('utf-8')).hexdigest() for val in data['md5']
    ]
    data = data.drop(['md5'], axis=1)
    ##生成入库时间
    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_daily_basic_index",
    config_schema={
        "mode": str,
        "ts_code": str,
        "start_date": str,
        "end_date": str,
    },
)
def tushare_daily_basic_index(context) -> pd.DataFrame:
    """
    每日PE等基本指标数据
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=32
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __start_date = context.op_config["start_date"]
    __end_date = context.op_config["end_date"]

    if __mode == 'daily':
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.query('daily_basic', trade_date=today)
    elif __mode == 'para':
        ##开始时间和结束时间都是必填
        data = pro.query(
            'daily_basic',
            ts_code=__ts_code,
            start_date=__start_date,
            end_date=__end_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="warehouse_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_forecast",
    config_schema={
        "mode": str,
        "ts_code": str,
        "ann_date": str,
        "period": str,
    },
)
def tushare_forecast(context) -> pd.DataFrame:
    """
    业绩预告
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=45
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __ann_date = context.op_config["ann_date"]
    __period = context.op_config["period"]

    if __mode == 'daily':
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.forecast_vip(ann_date=today)
    elif __mode == 'para':
        data = pro.forecast_vip(
            ts_code=__ts_code,
            period=__period,
            ann_date=__ann_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="warehouse_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_express",
    config_schema={
        "mode": str,
        "ts_code": str,
        "ann_date": str,
        "period": str,
    },
)
def tushare_express(context) -> pd.DataFrame:
    """
    业绩快报
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=45
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __ann_date = context.op_config["ann_date"]
    __period = context.op_config["period"]

    if __mode == 'daily':
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.express_vip(ann_date=today)
    elif __mode == 'para':
        data = pro.express_vip(
            ts_code=__ts_code,
            period=__period,
            ann_date=__ann_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_fina_indicator",
    config_schema={
        "mode": str,
        "ts_code": str,
        "ann_date": str,
        "start_date": str,
        "end_date": str,
        "period": str,
    },
)
def tushare_fina_indicator(context) -> pd.DataFrame:
    """
    财务指标表
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=79
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __ann_date = context.op_config["ann_date"]
    __start_date = context.op_config["start_date"]
    __end_date = context.op_config["end_date"]
    __period = context.op_config["period"]

    if __mode == "daily":
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.fina_indicator_vip(ann_date=today)
    elif __mode == "para":
        data = pro.fina_indicator_vip(
            ts_code=__ts_code,
            period=__period,
            ann_date=__ann_date,
            start_date=__start_date,
            end_date=__end_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    ##生成入库时间
    created_at = round(time())
    data['created_at'] = created_at
    return data


@asset(
    io_manager_key="tushare_pg_replace_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_convertible_bond_basic",
)
def tushare_convertible_bond_basic() -> pd.DataFrame:
    """
    tushare可转债列表
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=185
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    data = pro.cb_basic(
        fields="ts_code,bond_short_name,stk_code,stk_short_name,list_date,delist_date"
    )
    return data


@asset(
    io_manager_key="tushare_pg_append_io_manager",
    key_prefix="tushare",
    group_name="tushare",
    op_tags={"group": "EL"},
    name="tushare_convertible_bond_daily",
    config_schema={
        "mode": str,
        "ts_code": str,
        "start_date": str,
        "end_date": str,
    },
)
def tushare_convertible_bond_daily(context) -> pd.DataFrame:
    """
    可转债每日行情
    :param context:
    :return:pandas.Dataframe，https://tushare.pro/document/2?doc_id=187
    """
    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    __mode = context.op_config["mode"]
    __ts_code = context.op_config["ts_code"]
    __start_date = context.op_config["start_date"]
    __end_date = context.op_config["end_date"]

    if __mode == 'daily':
        today = date.strftime(date.today(), "%Y%m%d")
        data = pro.cb_daily(trade_date=today)
    elif __mode == 'para':
        ##开始时间和结束时间都是必填
        data = pro.cb_daily(
            ts_code=__ts_code,
            start_date=__start_date,
            end_date=__end_date,
        )
    else:
        ValueError("Unsupported value: " + str(context.op_config["mode"]))

    created_at = round(time())
    data['created_at'] = created_at
    return data
