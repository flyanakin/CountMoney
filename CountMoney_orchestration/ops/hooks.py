import json
import os

import requests
from dagster import success_hook, failure_hook, HookContext
from CountMoney_orchestration.resources import WECOM_BOT_TOKEN_KIKYO

wecom_bot_token = WECOM_BOT_TOKEN_KIKYO


@success_hook
def wecom_bot_message_on_success(context: HookContext):
    message = f"{context.job_name} finished successfully，赶紧去数仓数钱吧"
    context.log.info(message)
    __alert(message)


@failure_hook
def wecom_bot_message_on_failure(context: HookContext):
    """任一个op失败发失败通知"""
    message = f"Error: Op {context.op.name} failed"
    context.log.info(message)
    __alert(message)


def __alert(msg: str):
    hook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={wecom_bot_token}"
    post_data = {"msgtype": "markdown", "markdown": {"content": msg}}

    headers = {"Content-Type": "application/json"}
    requests.post(
        url=hook_url,
        headers=headers,
        data=json.dumps(post_data),
    )
