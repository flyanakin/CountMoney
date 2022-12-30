from dagster import load_assets_from_modules, repository, with_resources
from CountMoney_orchestration.assets import tushare
from CountMoney_orchestration.jobs.load_basic_info import load_basic_info
from CountMoney_orchestration.jobs.demo import demo
from CountMoney_orchestration.jobs.load_history_data import load_history_data
from CountMoney_orchestration.jobs.daily_asset_job import daily_asset_job
from CountMoney_orchestration.resources import resources_prod

all_assets = load_assets_from_modules([tushare])
all_jobs = [load_basic_info, demo, load_history_data, daily_asset_job]


@repository
def CountMoney_orchestration():
    definitions = [with_resources(all_assets, resource_defs=resources_prod)] + all_jobs
    return definitions
