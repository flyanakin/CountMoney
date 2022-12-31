from dagster import define_asset_job

daily_asset_job = define_asset_job(
    name="daily_asset_job", selection=["daily_basic_index"]
)
