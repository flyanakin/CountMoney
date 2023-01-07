from dagster import (
    load_assets_from_modules,
    repository,
    with_resources,
    ScheduleDefinition,
    DefaultScheduleStatus,
)
import os
from dagster_dbt import load_assets_from_dbt_project
from CountMoney_orchestration.assets import tushare
from CountMoney_orchestration.jobs.load_basic_info import load_basic_info
from CountMoney_orchestration.jobs.demo import demo
from CountMoney_orchestration.jobs.load_history_data import load_history_data
from CountMoney_orchestration.jobs.daily_assets_job import daily_assets_job
from CountMoney_orchestration.resources import (
    resources_prod,
    dbt_resource_def,
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
)


@repository
def CountMoney_orchestration():
    dbt_assets = with_resources(
        load_assets_from_dbt_project(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
        resource_defs=dbt_resource_def,
    )

    tushare_assets = with_resources(
        load_assets_from_modules(modules=[tushare]),
        resource_defs=resources_prod,
    )

    all_assets = [dbt_assets, tushare_assets]

    all_jobs = [demo, daily_assets_job]

    all_schedules = [
        ScheduleDefinition(
            cron_schedule="0 18 * * *",
            job=daily_assets_job,
            environment_vars=dict(os.environ),
            execution_timezone="Asia/Shanghai",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ]

    definitions = all_assets + all_jobs + all_schedules
    return definitions
