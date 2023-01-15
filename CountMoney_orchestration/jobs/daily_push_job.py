from dagster import job
from CountMoney_orchestration.ops.push import send_email, message_generate


@job()
def daily_push_job():
    send_email(message_generate())
