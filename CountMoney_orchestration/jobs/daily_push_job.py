from dagster import (
    graph,
    asset_sensor,
    RunRequest,
    AssetKey,
    EventLogEntry,
    SensorEvaluationContext,
)
from CountMoney_orchestration.ops.analysis import read_table, portfolio_analysis
from CountMoney_orchestration.ops.push import (
    send_email,
    send_wecom_bot,
)

default_config = {
    "ops": {
        "read_table": {
            "config": {
                "database": "warehouse",
                "schema": "finance",
                "table": "portfolio",
            }
        }
    }
}


@graph()
def push_analysis_result():
    message = portfolio_analysis(read_table())
    send_wecom_bot(message)


daily_push_job = push_analysis_result.to_job(
    name='daily_push_job', config=default_config
)
