# teams_alerter.py
import requests
import logging

log = logging.getLogger("monitor_system")

def send_teams_alert(teams_cfg: dict, title: str, text: str, severity: str = "info", timeout=8):
    """
    向 Teams 发送 Adaptive Card 格式的日报，保持原始排版。
    """
    if not teams_cfg or not teams_cfg.get("ENABLED"):
        return
    url = teams_cfg.get("FLOW_URL")
    if not url:
        log.warning("TEAMS_NOTIFY 启用但缺少 FLOW_URL，跳过发送。")
        return

    # ✅ Adaptive Card payload
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": f"{title}",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"```\n{text}\n```",
                            "wrap": True,
                            "fontType": "Monospace",
                            "spacing": "Medium"
                        }
                    ]
                }
            }
        ]
    }

    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code >= 300:
            log.error(f"[TEAMS] 发送失败 HTTP {r.status_code}: {r.text[:300]}")
        else:
            log.info("[TEAMS] 发送成功。")
    except Exception as e:
        log.exception(f"[TEAMS] 请求异常: {e}")
