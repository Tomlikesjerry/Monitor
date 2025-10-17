# teams_alerter.py
import requests, logging

log = logging.getLogger("monitor_system")

def send_teams_alert(teams_cfg: dict, title: str, text: str, severity: str = "info", timeout=8):
    """
    直发 Power Automate（Teams Workflow）HTTP 触发器。
    Flow 触发器的 JSON Schema 建议包含 title/text/severity 三个字段。
    """
    if not teams_cfg or not teams_cfg.get("ENABLED"):
        return
    url = teams_cfg.get("FLOW_URL")
    if not url:
        log.warning("TEAMS_NOTIFY 启用但缺少 FLOW_URL，跳过发送。")
        return
    payload = {"title": title, "text": text, "severity": severity}
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code >= 300:
            log.error(f"[TEAMS] 发送失败 HTTP {r.status_code}: {r.text[:300]}")
        else:
            log.info("[TEAMS] 发送成功。")
    except Exception as e:
        log.exception(f"[TEAMS] 请求异常: {e}")
