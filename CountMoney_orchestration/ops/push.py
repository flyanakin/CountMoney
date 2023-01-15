import pandas as pd
from dagster import op
import smtplib
from email.mime.text import MIMEText
from CountMoney_orchestration.resources import (
    MAIL_USER,
    MAIL_PASS,
    MAIL_RECEIVER,
)


@op()
def message_generate():
    message = """
    组合表现：
    · 双增组合持仓收益：10%
    · 低估值蓝筹持仓收益：20%
    
    止盈提醒：
    · 二段阶梯（56%->43.75%）：宁德时代（43.75%）
    · 三段阶梯（95%->79.68%）: 东方电缆（79.68%）
    · 四段阶梯（144%->124.6%）: 光大证券（124.6%）
    · 换手率止盈：金雷股份（120亿市值，今日换手率20%）
    
    止损提醒：
    · 组合止损：双增组合（-8%）
    · 组合剪枝：隆基股份（双增组合，-15%）
    · 个股止损：阳光电源（-8%）
    """
    return message


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
