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
        if (
            value.profit_ratio > 0.1
            and value.sub_portfolio != '其他'
            and value.asset_type == '股票'
        ):
            profit = round(value.profit_ratio * 100, 2)
            stage, signal = moving_sell_strategy(value, pro)
            context.log.info(f"{value.asset_name} {stage} {signal}")
            sell_str = f"{value.asset_name}(阶梯{stage}，持仓收益{profit}%，{signal})"
            sell_result = sell_result + sell_str + "\n"
            if stage > 0:
                turnover_signal, total_mv, turnover_rate = turnover_sell_strategy(
                    value, pro
                )
                turn_str = f"{value.asset_name}(持仓收益{profit}%，市值{total_mv}亿,今日换手率{turnover_rate}%，{turnover_signal})"
                context.log.info(f"{total_mv},{turnover_rate}")
                turn_result = turn_result + turn_str + "\n"

    ##组合剪枝计算
    stock_cut_l = []
    for row in portfolio.itertuples():
        if row.sub_portfolio != '其他':
            if row.profit_ratio < -0.15:
                stock_cut_str = f"{row.asset_name}({row.sub_portfolio},{round(row.profit_ratio*100, 2)})"
                cut_l.append(stock_cut_str)
            else:
                stock_cut_l = '无'
        elif row.asset_type == '债券':
            if row.profit_ratio < -0.08:
                stock_cut_str = f"{row.asset_name}({row.sub_portfolio},{round(row.profit_ratio*100, 2)})"
                cut_l.append(stock_cut_str)
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
    """
    业绩预告分析
    :param context:
    :param df:
    :return:
    """
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
    """
    业绩快报分析
    :param context:
    :param df:
    :return:
    """
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


@op()
def index_monitor(context):
    """
    监控股票指数
    :param context:
    :return: message
    """
    today = date.strftime(date.today(), "%Y-%m-%d")
    ts.set_token(f"{TUSHARE_TOKEN}")
    pro = ts.pro_api()

    # todo:后续要做抽象
    # 取数，因为要ma60，所以需要取一段时间的数据，写一个差不多够远的数字就可以了
    df_sse50 = ts.pro_bar(
        ts_code='000016.SH',
        asset='I',
        freq='D',
        start_date='20220930',
        end_date=today,
        ma=[60],
    )

    df_gei = ts.pro_bar(
        ts_code='399006.SZ',
        asset='I',
        freq='D',
        start_date='20220930',
        end_date=today,
        ma=[60],
    )

    # 上证指数和深成指不用算背离率，所以取数距离短一点
    df_sse = ts.pro_bar(
        ts_code='000001.SH',
        asset='I',
        start_date='20230101',
        end_date=today,
    )

    df_szse = ts.pro_bar(
        ts_code='399001.SZ',
        asset='I',
        start_date='20230101',
        end_date=today,
    )
    # 上证50和创业版指数
    sse50 = round(df_sse50['close'].values[0], 2)
    gei = round(df_gei['close'].values[0], 2)

    # 计算60日背离率
    sse50_ma60 = df_sse50['ma60'].values[0]
    sse50_60bias = round((sse50 - sse50_ma60) / sse50_ma60 * 100, 2)

    gei_ma60 = df_gei['ma60'].values[0]
    gei_60bias = round((gei - gei_ma60) / gei_ma60 * 100, 2)

    # 两市成交额
    sse_amount = df_sse['amount'].values[0]
    sse_pct_chg = round(df_sse['pct_chg'].values[0], 2)

    szse_amount = df_szse['amount'].values[0]
    szse_pct_chg = round(df_szse['pct_chg'].values[0], 2)

    total_amount = round((sse_amount + szse_amount) / 100000)

    message = f"""
{today} 春季行情监控，
保守：满足一个指标即可降至半仓
激进：大多数指标满足再行动
1、上证50偏离60日均线达到15%（上证50：{sse50_60bias}%，创业板：{gei_60bias}%）
2、上证50达到3000～3100点（今天：{sse50}），或创业板指达到2800～2900点（今天：{gei}）
3、大盘上涨时成交额达到1.3万亿，或下跌时达到1.7万亿（今天上证{sse_pct_chg}%，深证{szse_pct_chg}%，总成交额{total_amount}亿）
"""
    context.log.info(message)
    return message
