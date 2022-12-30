from dagster import define_asset_job, AssetSelection, materialize
from CountMoney_orchestration.assets.tushare import balance_sheet
from CountMoney_orchestration.resources import resources_prod


load_history_data = define_asset_job(
    name="load_history_data",
    selection=AssetSelection.keys("balance_sheet"),
    config={"ops": {"balance_sheet": {"config": {"mode": "history"}}}},
)
