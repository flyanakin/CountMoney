from dagster import define_asset_job, AssetSelection

daily_config = {
    "ops": {
        "tushare__tushare_balance_sheet": {
            "config": {
                "mode": "daily",
                "ts_code": "",
                "period": "",
                "ann_date": "",
                "start_date": "",
                "end_date": "",
            }
        },
        "tushare__tushare_income_statement": {
            "config": {
                "mode": "daily",
                "ts_code": "",
                "period": "",
                "ann_date": "",
                "start_date": "",
                "end_date": "",
            }
        },
    }
}


daily_assets_job = define_asset_job(
    name="daily_asset_job",
    selection=AssetSelection.keys(["stock_picks"]).upstream(),
    config=daily_config,
)
