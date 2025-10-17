# teams_alerter.py
# 发送报警/日报到 Microsoft Teams 频道邮箱（邮件方式）
# 依赖：Python 标准库（smtplib、email），不需要额外安装第三方库
import logging
import time
import smtplib
import threading
from email.mime.text import MIMEText
from html import escape

logger = logging.getLogger("monitor_system")

# 进程内的简易限流与去重缓存
_lock = threading.Lock()
_sent_times = []         # 最近 60 秒内的发送时间戳
_dedup_cache = {}        # dedup_key -> last_sent_ts

# -----------------------------
# 内部工具
# -----------------------------
def _rate_limit(max_per_minute: int):
    """本地软限流：每分钟最多 N 封（超过则等待窗口滑出）。"""
    if max_per_minute <= 0:
        return
    now = time.time()
    with _lock:
        # 清理 60s 以前的发送记录
        while _sent_times and (now - _sent_times[0] > 60):
            _sent_times.pop(0)
        if len(_sent_times) >= max_per_minute:
            wait_s = 60 - (now - _sent_times[0])
        else:
            wait_s = 0
    if wait_s > 0:
        time.sleep(wait_s)

def _should_dedup(dedup_key: str, window_minutes: int) -> bool:
    """
    时间窗内去重：同一 dedup_key 在 window_minutes 分钟内只发一次。
    返回 True 表示应跳过（视为已发）。
    """
    if not dedup_key or window_minutes <= 0:
        return False
    now = time.time()
    with _lock:
        last = _dedup_cache.get(dedup_key, 0)
        if now - last < window_minutes * 60:
            return True
        _dedup_cache[dedup_key] = now
    return False

def _markdown_to_html(md: str) -> str:
    """
    轻量 Markdown → HTML：支持 **加粗**、*斜体*、换行。
    如需更复杂的 Markdown，可替换为完整渲染器。
    """
    if not md:
        return ""
    s = escape(md)  # 先整体转义，避免 XSS
    # 处理 **bold**
    s = s.replace("&ast;&ast;", "§§B§§")  # 暂存占位，避免与单星冲突
    while "§§B§§" in s:
        s = s.replace("§§B§§", "<b>", 1)
        s = s.replace("§§B§§", "</b>", 1)
    # 处理 *italic*
    s = s.replace("&ast;", "§I§")
    while "§I§" in s:
        s = s.replace("§I§", "<i>", 1)
        s = s.replace("§I§", "</i>", 1)
    # 换行
    s = s.replace("\n", "<br/>")
    return s

def _send_email(smtp_cfg: dict, to_addr: str, subject: str, html: str) -> bool:
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_cfg["USER"]
    msg["To"] = to_addr

    try:
        with smtplib.SMTP(smtp_cfg["HOST"], int(smtp_cfg.get("PORT", 587))) as s:
            s.starttls()
            s.login(smtp_cfg["USER"], smtp_cfg["PASS"])
            s.sendmail(smtp_cfg["USER"], [to_addr], msg.as_string())
        with _lock:
            _sent_times.append(time.time())
        return True
    except Exception as e:
        logger.error(f"[TeamsEmail] 发送失败: {e}", exc_info=True)
        return False

# -----------------------------
# 对外主函数（与你的监控/日报脚本集成）
# -----------------------------
def send_teams_alert(config: dict, title: str, text: str, *,
                     silent: bool = False,
                     dedup_key: str = None) -> bool:
    """
    发送一条消息到 Teams 频道（通过频道邮箱）。
    - config: 你的全局配置 dict（从 config.toml 读入）
      需要字段：
        [TEAMS_NOTIFY]
          PROVIDER="email"
          CHANNEL_EMAIL="xxxx@apac.teams.ms"
        [SMTP]
          HOST="smtp.office365.com"
          PORT=587
          USER="bot@yourdomain.com"
          PASS="StrongPassword"
        [ALERTING]（可选）
          MAX_EMAILS_PER_MINUTE=20
          DEDUP_WINDOW_MINUTES=15
    - title: 邮件主题（频道里显示）
    - text : Markdown/纯文本内容
    - silent: 占位（邮件无“静默”概念），保留以兼容你的原形参
    - dedup_key: 去重键（例如 "VOL|BTCUSDT|2025-10-12T10:15Z"）
    """
    tn = (config or {}).get("TEAMS_NOTIFY", {}) or {}
    smtp_cfg = (config or {}).get("SMTP", {}) or {}
    alerting = (config or {}).get("ALERTING", {}) or {}

    provider = (tn.get("PROVIDER") or "").lower()
    if provider != "email":
        logger.error("TEAMS_NOTIFY.PROVIDER 必须为 'email' 才能使用邮件通道。")
        return False

    channel_email = tn.get("CHANNEL_EMAIL")
    if not channel_email:
        logger.error("TEAMS_NOTIFY.CHANNEL_EMAIL 未配置。")
        return False

    for k in ("HOST", "PORT", "USER", "PASS"):
        if not smtp_cfg.get(k):
            logger.error(f"SMTP.{k} 未配置。")
            return False

    # 时间窗去重
    dedup_win = int(alerting.get("DEDUP_WINDOW_MINUTES", 15))
    if _should_dedup(dedup_key, dedup_win):
        logger.info(f"[TeamsEmail] 去重命中，跳过发送。dedup_key={dedup_key}")
        return True

    # 本地软限流
    _rate_limit(int(alerting.get("MAX_EMAILS_PER_MINUTE", 20)))

    # 轻量 Markdown → HTML
    html_body = f"<b>{escape(title)}</b><br/>{_markdown_to_html(text)}"
    ok = _send_email(smtp_cfg, channel_email, subject=title, html=html_body)
    if ok:
        logger.info(f"[TeamsEmail] 已发送到频道邮箱: {channel_email} | 主题: {title}")
    return ok
