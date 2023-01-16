from dagster import (
    RunRequest,
    asset_sensor,
    AssetKey,
    EventLogEntry,
    SensorEvaluationContext,
)


@asset_sensor(
    asset_key=AssetKey("portfolio"),
    job_name='daily_push_job',
    minimum_interval_seconds=300,
)
def portfolio_update_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    yield RunRequest(run_key=None)
