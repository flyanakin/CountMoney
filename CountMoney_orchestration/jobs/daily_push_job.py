from dagster import graph
from CountMoney_orchestration.ops.analysis import (
    read_table,
    portfolio_analysis,
    performance_analysis,
    preview_analysis,
    index_monitor,
)
from CountMoney_orchestration.ops.push import send_wecom_bot, send_wecom_bot_queue

portfolio_config = {
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

performance_config = {
    "ops": {
        "read_table": {
            "config": {
                "database": "warehouse",
                "schema": "finance",
                "table": "performance",
            }
        }
    }
}

preview_config = {
    "ops": {
        "read_table": {
            "config": {
                "database": "warehouse",
                "schema": "finance",
                "table": "preview",
            }
        }
    }
}


@graph()
def push_portfolio_analysis_result():
    message = portfolio_analysis(read_table())
    send_wecom_bot_queue(message)


portfolio_push_job = push_portfolio_analysis_result.to_job(
    name='portfolio_push_job', config=portfolio_config
)


@graph()
def push_performance_analysis_result():
    message = performance_analysis(read_table())
    send_wecom_bot_queue(message)


performance_push_job = push_performance_analysis_result.to_job(
    name='performance_push_job', config=performance_config
)


@graph()
def push_preview_analysis_result():
    message = preview_analysis(read_table())
    send_wecom_bot_queue(message)


preview_push_job = push_preview_analysis_result.to_job(
    name='preview_push_job', config=preview_config
)


@graph()
def index_monitor_result():
    message = index_monitor()
    send_wecom_bot_queue(message)


index_monitor_job = index_monitor_result.to_job(
    name='index_monitor_job',
)
