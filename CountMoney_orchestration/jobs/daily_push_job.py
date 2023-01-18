from dagster import graph
from CountMoney_orchestration.ops.analysis import read_table, portfolio_analysis
from CountMoney_orchestration.ops.push import send_wecom_bot, send_wecom_bot_queue

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
def push_portfolio_analysis_result():
    message = portfolio_analysis(read_table())
    send_wecom_bot_queue(message)


portfolio_push_job = push_portfolio_analysis_result.to_job(
    name='portfolio_push_job', config=default_config
)
