import json
import requests
from dagster import op
import smtplib
from email.mime.text import MIMEText
from CountMoney_orchestration.resources import MAIL_USER, MAIL_PASS, MAIL_RECEIVER
from CountMoney_orchestration.resources import (
    WECOM_BOT_TOKEN_KIKYO,
    WECOM_BOT_TOKEN_MIKOTO,
)

wecom_bot_token = WECOM_BOT_TOKEN_MIKOTO
wecom_queue = [WECOM_BOT_TOKEN_KIKYO,WECOM_BOT_TOKEN_MIKOTO]


@op()
def send_email(email_context: str) -> None:
    mail_host = "smtp.qq.com"
    mail_user = MAIL_USER
    mail_pass = MAIL_PASS
    sender = MAIL_USER
    # 邮件接受方邮箱地址，注意需要[]包裹，这意味着你可以写多个邮件地址群发
    receivers = [MAIL_RECEIVER]

    # 设置email信息
    # 邮件内容设置
    message = MIMEText(email_context, 'plain', 'utf-8')
    # 邮件主题
    message['Subject'] = '股市速递'
    # 发送方信息
    message['From'] = sender
    # 接受方信息
    message['To'] = receivers[0]

    # 登录并发送邮件
    try:
        smtp_obj = smtplib.SMTP()
        # 连接到服务器
        smtp_obj.connect(mail_host, 25)
        # 登录到服务器
        smtp_obj.login(mail_user, mail_pass)
        # 发送
        smtp_obj.sendmail(sender, receivers, message.as_string())
        # 退出
        smtp_obj.quit()
        print('success')
    except smtplib.SMTPException as e:
        print('error', e)  # 打印错误


@op()
def send_wecom_bot(message: str):
    hook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={wecom_bot_token}"
    post_data = {"msgtype": "text", "text": {"content": message}}

    headers = {"Content-Type": "application/json"}
    requests.post(
        url=hook_url,
        headers=headers,
        data=json.dumps(post_data),
    )

@op()
def send_wecom_bot_queue(context,message: str):
    for token in wecom_queue:
        hook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={token}"
        post_data = {"msgtype": "text", "text": {"content": message}}
        headers = {"Content-Type": "application/json"}
        requests.post(
            url=hook_url,
            headers=headers,
            data=json.dumps(post_data),
        )