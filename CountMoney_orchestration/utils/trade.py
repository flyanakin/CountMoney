import pandas as pd
import tushare as ts
from datetime import date
from datetime import datetime,timedelta
import math


def moving_sell_strategy(stock: pd.Series, pro):
    ##取最新数据，这里要取最新是因为排除复权问题
    start_date = date.strftime(stock.order_date, "%Y%m%d")
    today = date.strftime(date.today(), "%Y%m%d")
    df = pro.query(
        'daily', ts_code=stock.stock_code, start_date=start_date, end_date=today
    )
    data = df.set_index("trade_date")
    max = data['close'].max()

    profit_ratio = (stock["last"] - stock.cost) / stock.cost
    max_ratio = (max - stock.cost) / stock.cost
    ##计算处于哪个阶梯，25%为一段，下跌8%止盈
    stage = int(math.log((1 + max_ratio), 1.25))
    if stock["last"] < 0.92*(math.pow(1.25,stage)):
        signal = 'sell'
    else:
        signal = 'hold'
    

    return stage,signal


def turnover_sell_strategy(stock:pd.Series,pro):
    today = date.strftime(date.today(), "%Y%m%d")
    start_date = (datetime.now()-timedelta(days=45)).strftime("%Y%m%d")
    df = pro.query('daily_basic',ts_code=stock.stock_code,start_date=start_date,end_date=today)
    df.sort_values(by=['trade_date'],inplace=True,ascending=False)
    total_mv = df.loc[0,'total_mv']/10000
    turnover_rate = df.loc[0,'turnover_rate']

    if total_mv <= 50 and turnover_rate > 35:
        signal = 'sell'
    elif 50 < total_mv <= 100 and turnover_rate > 25:
        signal = 'sell'
    elif 100 < total_mv <= 200 and turnover_rate > 15:
        signal = 'sell'
    elif 200 < total_mv <= 400 and turnover_rate > 10:
        signal = 'sell'
    elif total_mv > 400 and turnover_rate > 7:
        signal = 'sell'
    else:
        signal = 'hold'

    return signal, round(total_mv), turnover_rate


