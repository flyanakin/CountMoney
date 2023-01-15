from dagster import asset
from pyairtable import Table
import pandas as pd
from CountMoney_orchestration.resources import AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID
from time import time


@asset(
    io_manager_key="warehouse_pg_append_io_manager",
    key_prefix="manual_update",
    group_name="airtable",
    op_tags={"group": "EL"},
    name="airtable_portfolio",
)
def airtable_portfolio() -> pd.DataFrame:
    """
    持仓数据
    :param context:
    :return:pandas.Dataframe，{stock_code, stock_name, position, cost, group}
    """
    api_key = AIRTABLE_API_TOKEN
    base_id = AIRTABLE_BASE_ID
    table = Table(api_key, base_id, 'portfolio')
    data = table.all()
    df = pd.DataFrame.from_records((r['fields'] for r in data))
    created_at = round(time())
    df['created_at'] = created_at
    return df
