import pandas as pd
import tushare as ts
from dagster import op
from sqlalchemy import create_engine
from datetime import date
from CountMoney_orchestration.utils.trade import (
    moving_sell_strategy,
    turnover_sell_strategy,
)

##因为inputManager功能还在实验阶段，先用强撸的方式实现
from CountMoney_orchestration.resources import (
    WAREHOUSE_USER,
    WAREHOUSE_HOSTS,
    WAREHOUSE_SECRET,
    TUSHARE_TOKEN,
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

    pro = ts.pro_api(f"{TUSHARE_TOKEN}")
    ## 按组合group by，并计算组合收益
    pivoted_to_p = portfolio.groupby(["sub_portfolio"]).sum()
    pivoted_to_p['sub_portfolio_profit'] = (
        pivoted_to_p['profit'] / pivoted_to_p['total_cost']
    )

    ##组合概览
    # 仓位计算
    condition = portfolio['sub_portfolio']
    total_asset = portfolio.loc[:, 'market_capitalization'].sum()
    total_position = (
        portfolio.loc[condition != '其他', 'market_capitalization'].sum() / total_asset
    )

    portfolio_profit_str = ""
    cut_l = []
    for row in pivoted_to_p.itertuples():
        if row.Index != '其他':
            sub_portfolio_position = row.market_capitalization / total_asset
            profit_str = f"· {row.Index}组合：持仓收益{round(row.sub_portfolio_profit*100, 2)}%，仓位：{round(sub_portfolio_position*100, 2)}%"
            portfolio_profit_str = portfolio_profit_str + profit_str + '\n'
            if row.sub_portfolio_profit < -0.08:
                cut_str = f"{row.Index}组合（{round(row.sub_portfolio_profit*100, 2)}%）"
                cut_l.append(cut_str)
            else:
                cut_loss_str = '无'
    cut_loss_str = "、".join(cut_l)

    ##止盈计算
    sell_result = ""
    turn_result = ""
    for index, value in portfolio.iterrows():
        if value.profit_ratio > 0.1 and value.sub_portfolio != '其他':
            profit = round(value.profit_ratio * 100, 2)
            stage, signal = moving_sell_strategy(value, pro)
            context.log.info(f"{value.stock_name} {stage} {signal}")
            sell_str = f"{value.stock_name}(阶梯{stage}，持仓收益{profit}%，{signal})"
            sell_result = sell_result + sell_str + "\n"
            if stage > 0:
                turnover_signal, total_mv, turnover_rate = turnover_sell_strategy(
                    value, pro
                )
                turn_str = f"{value.stock_name}(持仓收益{profit}%，市值{total_mv}亿,今日换手率{turnover_rate}%，{turnover_signal})"
                context.log.info(f"{total_mv},{turnover_rate}")
                turn_result = turn_result + turn_str + "\n"

    ##组合剪枝计算
    stock_cut_l = []
    for row in portfolio.itertuples():
        if row.sub_portfolio != '其他':
            if row.profit_ratio < -0.15:
                stock_cut_str = f"{row.stock_name}({row.sub_portfolio},{round(row.profit_ratio*100, 2)})"
                cut_l.append(stock_cut_str)
            else:
                stock_cut_l = '无'
    stock_cut_l = "、".join(cut_l)

    message = f"""
组合概览表现：
· 总仓位：{round((total_position * 100),2)}%
{portfolio_profit_str}

止盈提醒：
· 25/8移动止盈：
{sell_result}
· 换手率止盈：
{turn_result}

止损提醒：
· 组合止损：{cut_loss_str}
· 组合剪枝：{stock_cut_l}
"""
    context.log.info(message)
    return message


@op()
def performance_analysis(context, df: pd.DataFrame):
    today = date.strftime(date.today(), "%Y-%m-%d")
    context.log.info(today)
    selected = df.loc[
        (df['pe_ttm_max'] < 50)
        & (df['peg_max'] < 1)
        & (df['peg_max'] > 0)
        & (df['peg_min'] > 0)
        & (df['growth_net_income_quarterly_ratio_min'] > 0.35)
        & (df['ann_date'] == today)
    ]

    result = ""
    for index, value in selected.iterrows():
        performance_str = f"{value.stock_name}:净利润增长({round(value.growth_net_income_quarterly_ratio_min*100,2)}%~{round(value.growth_net_income_quarterly_ratio_max*100,2)}%)，peg({value.peg_min}~{value.peg_max})"
        result = result + performance_str + "\n"

    if result == "":
        result = "今日没有好业绩预告，搬砖去吧"

    message = f"""
{today} 业绩预告：
{result}
"""
    context.log.info(message)
    return message


@op()
def preview_analysis(context, df: pd.DataFrame):
    today = date.strftime(date.today(), "%Y-%m-%d")
    selected = df.loc[
        (df['pe_ttm'] < 50)
        & (df['peg_by_net_income'] < 1)
        & (df['growth_net_income_quarterly_ratio'] > 0.5)
        & (df['growth_total_revenue_quarterly_ratio'] > 0.2)
        & (df['ann_date'] == today)
    ]

    result = ""
    for index, value in selected.iterrows():
        p_str = f"{value.stock_name}:净利润增长({round(value.growth_net_income_quarterly_ratio*100,2)}%),营收增长({round(value.growth_total_revenue_quarterly_ratio*100,2)}%)，营收peg({value.peg_by_revenue})，净利润peg({value.peg_by_net_income})"
        result = result + p_str + "\n"

    if result == "":
        result = "今日没有好业绩快报，搬砖去吧"

    message = f"""
{today} 业绩快报：
{result}
"""
    context.log.info(message)
    return message
