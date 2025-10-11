# lark_alerter.py

import requests
import logging
import json
import time
import re

logger = logging.getLogger('monitor_system')

_ACCESS_TOKEN = {'token': None, 'expires_at': 0, 'app_id': None}

def _base_urls(lark_region: str):
    """根据租户区域返回正确域名；larksuite=国际版，feishu=中国大陆版"""
    if str(lark_region).lower() in ('feishu', 'cn', 'china', 'feishu_cn', 'cn_mainland'):
        auth = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        im   = "https://open.feishu.cn/open-apis/im/v1/messages"
    else:
        auth = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
        im   = "https://open.larksuite.com/open-apis/im/v1/messages"
    return auth, im

def _get_access_token(app_id, app_secret, lark_region='larksuite'):
    global _ACCESS_TOKEN
    if (_ACCESS_TOKEN['token'] and
        _ACCESS_TOKEN['expires_at'] > time.time() + 60 and
        _ACCESS_TOKEN['app_id'] == app_id):
        return _ACCESS_TOKEN['token']

    logger.info("Access Token 缓存失效，准备向 Lark 获取新的 Token。")
    auth_url, _ = _base_urls(lark_region)
    payload = {"app_id": app_id, "app_secret": app_secret}

    try:
        resp = requests.post(auth_url, json=payload, timeout=12)
        data = resp.json()  # 不先 raise，优先解析业务体
        if resp.status_code == 200 and data.get('code') == 0:
            token = data['tenant_access_token']
            expires_in = data['expire']
            _ACCESS_TOKEN.update({
                'token': token,
                'expires_at': time.time() + expires_in - 60,
                'app_id': app_id
            })
            logger.info("Lark Access Token 获取成功。")
            return token
        else:
            logger.error(f"获取 Token 失败。HTTP {resp.status_code}, "
                         f"Lark code={data.get('code')}, msg={data.get('msg')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"请求 Lark Token API 失败 (网络/超时): {e}")
        return None
    except Exception as e:
        logger.error(f"Token 获取过程中发生未知错误: {e}")
        return None

def _infer_receive_id_type(receive_id: str) -> str:
    """根据 receive_id 自动判断类型：chat_id/open_id/email/user_id"""
    if receive_id.startswith("oc_"):
        return "chat_id"
    if receive_id.startswith("ou_"):
        return "open_id"
    if "@" in receive_id:
        return "email"
    # 粗略判定 user_id：长度/字符集因租户而异，这里留兜底
    if re.fullmatch(r"[0-9a-zA-Z_-]{16,64}", receive_id):
        return "user_id"
    # 默认认为是 chat_id（与现有配置兼容）
    return "chat_id"

def send_lark_alert(lark_config, title, text):
    """
    使用 Tenant Access Token 发送 Lark 告警消息到指定对象（群聊/用户）。
    必填：APP_ID, APP_SECRET, ALERT_RECEIVE_ID
    可选：LARK_REGION = 'larksuite' | 'feishu'
    """
    app_id     = lark_config.get('APP_ID')
    app_secret = lark_config.get('APP_SECRET')
    receive_id = lark_config.get('ALERT_CHAT_ID') or lark_config.get('ALERT_RECEIVE_ID')
    lark_region = lark_config.get('LARK_REGION', 'larksuite')

    if not all([app_id, app_secret, receive_id]):
        logger.error("Lark 配置不完整 (App ID / Secret / Receive ID 缺失)。请检查 config.json。")
        return

    token = _get_access_token(app_id, app_secret, lark_region=lark_region)
    if not token:
        logger.error("无法获取 Access Token，告警发送失败。")
        return

    # 自动识别 receive_id_type（也支持你的原有 chat_id）
    receive_id_type = _infer_receive_id_type(receive_id)

    # 构造消息体：text 类型
    message_content = {"text": f"{title}\n\n{text}"}

    _, im_url = _base_urls(lark_region)
    url = f"{im_url}?receive_id_type={receive_id_type}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"receive_id": receive_id, "msg_type": "text", "content": json.dumps(message_content)}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=12)
        # 不立刻 raise，优先读业务体，便于排障
        try:
            data = resp.json()
        except Exception:
            data = {"parse_error": True, "text": resp.text}

        if resp.status_code == 200 and isinstance(data, dict) and data.get('code') == 0:
            logger.info(f"Lark 告警发送成功到 {receive_id_type}: {receive_id}. 标题: {title}")
            return

        # 打印尽可能多的诊断信息
        logger.error(
            f"Lark 发送失败。HTTP {resp.status_code}; "
            f"code={data.get('code')}; msg={data.get('msg')}; "
            f"receive_id_type={receive_id_type}; receive_id={receive_id}"
        )

        # 典型鉴权失效：下次强制刷新 token
        if isinstance(data, dict) and data.get('code') in (10500, 10501):
            _ACCESS_TOKEN['expires_at'] = 0

    except requests.exceptions.RequestException as e:
        logger.error(f"Lark 消息发送请求失败 (网络/超时): {e}. 标题: {title}")
    except Exception as e:
        logger.error(f"发送 Lark 告警时发生未知错误: {e}. 标题: {title}")
