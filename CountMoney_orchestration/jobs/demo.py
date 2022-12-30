import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from dagster import job, op
import os


@op
def get_stock_basic():
    """
    从tushare获取股票基础数据
    """
    tushare_token = os.getenv('TUSHARE_TOKEN')
    pro = ts.pro_api(f"{tushare_token}")
    data = pro.query('stock_basic', exchange='', list_status='L')
    return data


@op
def write_database(df):
    """
    写入数仓
    """
    warehouse_hosts = os.getenv('WAREHOUSE_HOSTS')
    warehouse_user = os.getenv('WAREHOUSE_USER')
    warehouse_secret = os.getenv('WAREHOUSE_SECRET')
    engine = create_engine(
        f"postgresql://{warehouse_user}:{warehouse_secret}@{warehouse_hosts}/datasources"
    )
    try:
        df.to_sql(
            'stock_basic', engine, index=False, schema='tushare', if_exists='append'
        )
    except Exception as e:
        print(e)


@job
def demo():
    ##先执行get_weibo_post_file，再执行change_time_format
    write_database(get_stock_basic())
