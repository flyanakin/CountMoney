from dagster import (
    RunRequest,
    asset_sensor,
    AssetKey,
    EventLogEntry,
    SensorEvaluationContext,
)


@asset_sensor(
    asset_key=AssetKey("portfolio"),
    job_name='portfolio_push_job',
    minimum_interval_seconds=300,
)
def portfolio_update_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    yield RunRequest(run_key=None)


@asset_sensor(
    asset_key=AssetKey("performance"),
    job_name='performance_push_job',
    minimum_interval_seconds=300,
)
def performance_update_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    yield RunRequest(run_key=None)


@asset_sensor(
    asset_key=AssetKey("preview"),
    job_name='preview_push_job',
    minimum_interval_seconds=300,
)
def preview_update_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    yield RunRequest(run_key=None)
