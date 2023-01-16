import pandas as pd
from dagster import op
from sqlalchemy import create_engine

##因为inputManager功能还在实验阶段，先用强撸的方式实现
from CountMoney_orchestration.resources import (
    WAREHOUSE_USER,
    WAREHOUSE_HOSTS,
    WAREHOUSE_SECRET,
)


@op(
    config_schema={
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
    hosts = WAREHOUSE_HOSTS
    user = WAREHOUSE_USER
    secret = WAREHOUSE_SECRET
    database = context.op_config['database']
    schema = context.op_config['schema']
    table = context.op_config['table']

    engine = create_engine(f"postgresql://{user}:{secret}@{hosts}/{database}")
    df = pd.read_sql_table(table_name=table, con=engine, schema=schema)
    return df


@op()
def portfolio_analysis(context, portfolio: pd.DataFrame):
    ## 按组合group by，并计算组合收益
    pivoted_to_p = portfolio.groupby(["sub_portfolio"]).sum()
    pivoted_to_p['sub_portfolio_profit'] = (
        pivoted_to_p['profit'] / pivoted_to_p['total_cost']
    )

    ##组合收益计算，止盈止损提醒
    portfolio_profit_str = ""
    cut_l = []
    for row in pivoted_to_p.itertuples():
        if row.Index != '其他':
            profit_str = (
                f"· {row.Index}组合持仓收益：{round(row.sub_portfolio_profit*100, 2)}%"
            )
            portfolio_profit_str = portfolio_profit_str + profit_str + '\n'
            if row.sub_portfolio_profit < -0.08:
                cut_str = f"{row.Index}组合（{round(row.sub_portfolio_profit*100, 2)}%）"
                cut_l.append(cut_str)
                cut_loss_str = "、".join(cut_l)
            else:
                cut_loss_str = '无'

    ##止盈计算

    ##组合剪枝计算
    stock_cut_l = []
    for row in portfolio.itertuples():
        if row.sub_portfolio != '其他':
            if row.profit_ratio < -0.15:
                stock_cut_str = f"{row.stock_name}({row.sub_portfolio},{round(row.profit_ratio*100, 2)})"
                cut_l.append(stock_cut_str)
                stock_cut_l = "、".join(cut_l)
            else:
                stock_cut_l = '无'

    message = f"""
组合表现：
{portfolio_profit_str}

止盈提醒：
· 无

止损提醒：
· 组合止损：{cut_loss_str}
· 组合剪枝：{stock_cut_l}
    """
    context.log.info(message)
    return message
