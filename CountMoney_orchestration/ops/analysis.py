import pandas as pd
from dagster import op
from sqlalchemy import create_engine


@op(
    config_schema={
        "hosts": str,
        "user": str,
        "secret": str,
        "database": str,
        "schema": str,
        "table": str,
    }
)
def read_table(context):
    """
    从数仓读取数据
    :param context:
    :return: pd.Dataframe
    """
    hosts = context.op_config['hosts']
    user = context.op_config['user']
    secret = context.op_config['secret']
    database = context.op_config['database']
    schema = context.op_config['schema']
    table = context.op_config['table']

    engine = create_engine(f"postgresql://{user}:{secret}@{hosts}/{database}")
    df = pd.read_sql_table(table_name=table, con=engine, schema=schema)
    context.log.debug(df)
    return df

@op()
def portfolio_analysis(context, portfolio:pd.DataFrame):
    pass