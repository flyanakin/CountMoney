from dagster import define_asset_job

load_basic_info = define_asset_job(
    name="load_basic_info",
    selection=["tushare_stock_basic", "tushare_trade_calendar"],
)
