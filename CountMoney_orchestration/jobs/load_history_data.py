from dagster import define_asset_job


load_history_data = define_asset_job(
    name="load_history_data",
    selection=["tushare_balance_sheet", "tushare_income_statement"],
    config={
        "ops": {
            "tushare_balance_sheet": {"config": {"mode": "history"}},
            "tushare_income_statement": {"config": {"mode": "history"}},
        }
    },
)
