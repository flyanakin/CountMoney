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
        "tushare__tushare_daily_basic_index": {
            "config": {
                "mode": "daily",
                "ts_code": "",
                "start_date": "",
                "end_date": "",
            }
        },
        "tushare__tushare_forecast": {
            "config": {
                "mode": "daily",
                "ts_code": "",
                "period": "",
                "ann_date": "",
            }
        },
        "tushare__tushare_express": {
            "config": {
                "mode": "daily",
                "ts_code": "",
                "period": "",
                "ann_date": "",
            }
        },
    }
}


daily_assets_job = define_asset_job(
    name="daily_assets_job",
    selection=AssetSelection.groups("marts").upstream(),
    config=daily_config,
)
