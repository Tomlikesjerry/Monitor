# email_channel_tester.py
import sys
import os
import argparse
import smtplib
import socket
import ssl
import traceback
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid

# 复用你现有的工具：从 TOML 读取配置
try:
    from utils import load_config
except Exception as e:
    print("FATAL: 无法导入 utils.load_config()，请确认 utils.py 存在且支持 TOML。错误：", e)
    sys.exit(2)

def pick_bool(d: dict, key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(v) if v is not None else default

def build_message(mail_from: str, rcpt_to: str, subject: str, body: str, as_html: bool) -> MIMEText:
    subtype = "html" if as_html else "plain"
    msg = MIMEText(body, _subtype=subtype, _charset="utf-8")
    msg["From"] = mail_from
    msg["To"] = rcpt_to
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    return msg

def try_send_smtp(
    host: str, port: int, username: str, password: str,
    mail_from: str, rcpt_to: str, msg: MIMEText,
    use_ssl: bool, use_starttls: bool, timeout: int
) -> None:
    """
    按优先顺序尝试发送：
      1) 明确要求 SSL -> SMTP_SSL
      2) 明确要求 STARTTLS -> SMTP + starttls
      3) 未指定时：先尝试 STARTTLS，再回退纯明文（有些内网中继不支持 TLS）
    """
    if use_ssl and use_starttls:
        raise RuntimeError("use_ssl 与 use_starttls 不能同时为 True")

    def smtp_login_and_send(server):
        if username:
            server.login(username, password or "")
        server.sendmail(mail_from, [rcpt_to], msg.as_string())

    # 1) 直接 SSL
    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as s:
            s.ehlo()
            smtp_login_and_send(s)
        return

    # 2) 明确 STARTTLS
    if use_starttls:
        with smtplib.SMTP(host, port, timeout=timeout) as s:
            s.ehlo()
            s.starttls(context=ssl.create_default_context())
            s.ehlo()
            smtp_login_and_send(s)
        return

    # 3) 未指定：先试 STARTTLS，失败再退明文
    try:
        with smtplib.SMTP(host, port, timeout=timeout) as s:
            s.ehlo()
            if s.has_extn('starttls'):
                s.starttls(context=ssl.create_default_context()); s.ehlo()
            if username:
                s.login(username, password or "")
            s.sendmail(mail_from, [rcpt_to], msg.as_string())
        return
    except Exception as e:
        print("[WARN] STARTTLS/自动方式失败，尝试纯明文连接… 失败原因：", repr(e))

    with smtplib.SMTP(host, port, timeout=timeout) as s:
        s.ehlo()
        if username:
            s.login(username, password or "")
        s.sendmail(mail_from, [rcpt_to], msg.as_string())

def main():
    parser = argparse.ArgumentParser(
        description="向 Teams 频道邮箱发送一封测试邮件，验证 SMTP 与收件地址是否可用。"
    )
    parser.add_argument("--to", help="收件邮箱（可覆盖 config.toml 的 TEAMS_NOTIFY.to[0]）")
    parser.add_argument("--from", dest="mail_from", help="发件邮箱（覆盖 config）")
    parser.add_argument("--subject", default="(Test) Monitor → Teams 邮件连通性验证")
    parser.add_argument("--body", default="这是一封测试邮件：如果你能在 Teams 频道里看到这封邮件，说明 SMTP 与频道地址可用。")
    parser.add_argument("--html", action="store_true", help="以 HTML 发送（默认纯文本）")
    parser.add_argument("--host", help="SMTP 服务器（覆盖 config）")
    parser.add_argument("--port", type=int, help="SMTP 端口（覆盖 config）")
    parser.add_argument("--user", help="SMTP 用户名（覆盖 config）")
    parser.add_argument("--password", help="SMTP 密码/应用专用密码（覆盖 config）")
    parser.add_argument("--ssl", action="store_true", help="强制 SMTPS(SSL) 方式（通常端口465）")
    parser.add_argument("--starttls", action="store_true", help="强制 STARTTLS 方式（通常端口587）")
    parser.add_argument("--timeout", type=int, default=15, help="SMTP 超时（秒）")
    parser.add_argument("--dry-run", action="store_true", help="只打印配置与即将发送的内容，不真正发送")
    args = parser.parse_args()

    # 读取配置
    cfg = load_config()
    smtp_cfg = (cfg.get("SMTP") or {})
    teams_cfg = (cfg.get("TEAMS_NOTIFY") or {})

    # 基础字段（允许命令行覆盖）
    host = args.host or smtp_cfg.get("host") or "smtp.office365.com"
    port = args.port or int(smtp_cfg.get("port") or 587)
    username = args.user or smtp_cfg.get("username") or smtp_cfg.get("user") or ""
    password = args.password or smtp_cfg.get("password") or ""
    use_ssl = args.ssl or pick_bool(smtp_cfg, "use_ssl", False)
    use_starttls = args.starttls or pick_bool(smtp_cfg, "starttls", True)

    # 收发地址
    mail_from = args.mail_from or teams_cfg.get("from") or username
    rcpt_to = args.to or (teams_cfg.get("to") or [None])[0]

    # 主题/前缀
    subject_prefix = teams_cfg.get("subject_prefix") or ""
    subject = (subject_prefix + " " if subject_prefix else "") + args.subject
    body = args.body

    # 打印配置（脱敏）
    print("=== EMAIL TEST CONFIG ===")
    print(f"SMTP host      : {host}")
    print(f"SMTP port      : {port}")
    print(f"SMTP username  : {username!r}")
    print(f"SMTP password  : {'<provided>' if bool(password) else '<empty>'}")
    print(f"TLS mode       : {'SSL' if use_ssl else ('STARTTLS' if use_starttls else 'auto')}")
    print(f"MAIL FROM      : {mail_from}")
    print(f"RCPT TO        : {rcpt_to}")
    print(f"Subject        : {subject}")
    print(f"Content type   : {'HTML' if args.html else 'Plain Text'}")
    print("=========================")

    if not rcpt_to:
        print("FATAL: 未提供收件邮箱。请在 config.toml 的 [TEAMS_NOTIFY] 中配置 to = [\"channel@xxx.onmicrosoft.com\"]，或使用 --to 覆盖。")
        sys.exit(3)
    if not mail_from:
        print("FATAL: 发件邮箱未知。请设置 TEAMS_NOTIFY.from 或 SMTP.username，或使用 --from 覆盖。")
        sys.exit(3)

    # 构造消息
    msg = build_message(mail_from, rcpt_to, subject, body, args.html)

    if args.dry_run:
        print("[DRY-RUN] 仅打印配置与构造的邮件，不发送。")
        sys.exit(0)

    # 发送
    try:
        try_send_smtp(
            host=host, port=port, username=username, password=password,
            mail_from=mail_from, rcpt_to=rcpt_to, msg=msg,
            use_ssl=use_ssl, use_starttls=use_starttls, timeout=args.timeout
        )
        print("✅ 已发送测试邮件。请在 Teams 频道中检查是否收到。")
        sys.exit(0)
    except (smtplib.SMTPAuthenticationError) as e:
        print("❌ 认证失败（SMTPAuthenticationError）。请检查用户名/密码或是否需要应用专用密码。")
        print(repr(e))
        sys.exit(10)
    except (smtplib.SMTPConnectError, socket.timeout, socket.gaierror) as e:
        print("❌ 网络或连接失败。请检查防火墙/网络连通/端口是否开放。")
        print(repr(e))
        sys.exit(11)
    except smtplib.SMTPException as e:
        print("❌ SMTP 协议错误：", repr(e))
        traceback.print_exc()
        sys.exit(12)
    except Exception as e:
        print("❌ 未知错误：", repr(e))
        traceback.print_exc()
        sys.exit(13)

if __name__ == "__main__":
    main()
