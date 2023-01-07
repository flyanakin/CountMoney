from dagster import define_asset_job, AssetSelection

daily_config = {
    "ops": {
        "tushare__tushare_balance_sheet": {"config": {"mode": "daily"}},
        "tushare__tushare_income_statement": {"config": {"mode": "daily"}},
    }
}


daily_assets_job = define_asset_job(
    name="daily_asset_job",
    selection=AssetSelection.keys(["stock_picks"]).upstream(),
    config=daily_config,
)
