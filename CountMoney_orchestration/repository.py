from dagster import (
    load_assets_from_modules,
    repository,
    with_resources,
    ScheduleDefinition,
    DefaultScheduleStatus,
)
import os
from dagster_dbt import load_assets_from_dbt_project
from CountMoney_orchestration.assets import tushare, airtable
from CountMoney_orchestration.jobs.demo import demo
from CountMoney_orchestration.jobs.daily_assets_job import daily_assets_job
from CountMoney_orchestration.jobs.daily_push_job import (
    daily_push_job,
)
from CountMoney_orchestration.sensors.warehouse_sensors import portfolio_update_sensor
from CountMoney_orchestration.resources import (
    resources_prod,
    dbt_resource_def,
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
)
from CountMoney_orchestration.ops.hooks import (
    wecom_bot_message_on_success,
    wecom_bot_message_on_failure,
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

    airtable_assets = with_resources(
        load_assets_from_modules(modules=[airtable]),
        resource_defs=resources_prod,
    )

    # 整合成所有的assets
    # 这里需要注意的是，都需要加星号，星号的意识是将数组平铺成一个个元素，最终的效果是update_daily_assets是List of AssetsDefinition类型
    update_daily_assets = [*dbt_assets, *tushare_assets]

    ##resolve成JobDefinition
    daily_assets_job_resolved = daily_assets_job.resolve(
        assets=update_daily_assets, source_assets=[]
    ).with_hooks(hook_defs={wecom_bot_message_on_success, wecom_bot_message_on_failure})

    all_assets = [*update_daily_assets, *airtable_assets]

    all_jobs = [demo, daily_assets_job_resolved, daily_push_job]

    all_sensors = [portfolio_update_sensor]

    all_schedules = [
        ScheduleDefinition(
            cron_schedule="0 18 * * *",
            job=daily_assets_job_resolved,
            environment_vars=dict(os.environ),
            execution_timezone="Asia/Shanghai",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ]

    definitions = all_assets + all_jobs + all_sensors + all_schedules
    return definitions
