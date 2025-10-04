import requests
import logging
import json
import time

# 假设主脚本已经配置了 logging
logger = logging.getLogger('monitor_system')

# 缓存 Access Token 的全局变量
_ACCESS_TOKEN = {
    'token': None,
    'expires_at': 0,
    'app_id': None
}


# --- 核心辅助函数：获取 Access Token ---
def _get_access_token(app_id, app_secret):
    """使用 App ID 和 App Secret 获取 Tenant Access Token，并进行缓存。"""
    global _ACCESS_TOKEN

    # 1. 检查缓存是否有效
    if (_ACCESS_TOKEN['token'] and
            _ACCESS_TOKEN['expires_at'] > time.time() + 60 and
            _ACCESS_TOKEN['app_id'] == app_id):
        return _ACCESS_TOKEN['token']

    # 2. 缓存无效或过期，重新请求
    logger.info("Access Token 已过期或未获取，正在请求新的 Lark Token...")

    # 🚨 更改为 Lark 域名
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"

    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }

    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        data = response.json()

        if data.get('code') == 0:
            token = data['tenant_access_token']
            expires_in = data['expire']

            _ACCESS_TOKEN['token'] = token
            _ACCESS_TOKEN['expires_at'] = time.time() + expires_in - 60
            _ACCESS_TOKEN['app_id'] = app_id
            logger.info("Lark Access Token 获取成功。")
            return token
        else:
            logger.error(f"获取 Token 失败。Lark Code: {data.get('code')}, Msg: {data.get('msg')}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"请求 Lark Token API 失败 (网络/超时): {e}")
        return None
    except Exception as e:
        logger.error(f"Token 获取过程中发生未知错误: {e}")
        return None


# --- 核心告警函数：使用 Token 发送消息 ---
def send_lark_alert(lark_config, title, text):
    """使用 Tenant Access Token 发送 Lark 告警消息到指定群聊。"""
    app_id = lark_config.get('APP_ID')
    app_secret = lark_config.get('APP_SECRET')
    chat_id = lark_config.get('ALERT_CHAT_ID')

    if not all([app_id, app_secret, chat_id]):
        logger.error("Lark 配置不完整 (App ID, Secret 或 Chat ID 缺失)。请检查 config.json。")
        return

    # 1. 获取 Access Token
    token = _get_access_token(app_id, app_secret)
    if not token:
        logger.error("无法获取 Access Token，告警发送失败。")
        return

    # 2. 构造消息体 (使用 POST 富文本格式)
    message_content = {
        "text": f"{title}\n\n{text}"
    }

    # 飞书 API V1/V2 的消息发送接口
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "receive_id": chat_id,
        "msg_type": "text",  # 🚨 核心修改：使用 'text' 类型
        "content": json.dumps(message_content)
    }

    # 3. 发送消息
    try:
        response = requests.post(
            f"{url}?receive_id_type=chat_id",
            headers=headers,
            json=payload,
            timeout=5
        )
        response.raise_for_status()

        feishu_data = response.json()
        if feishu_data.get('code') == 0:
            logger.info(f"Lark 告警发送成功到 Chat ID: {chat_id}. 标题: {title}")
        else:
            logger.error(
                f"Lark API 返回业务错误 Code: {feishu_data.get('code')}, Msg: {feishu_data.get('msg')}. 标题: {title}")
            if feishu_data.get('code') in [10500, 10501]:
                _ACCESS_TOKEN['expires_at'] = 0

    except requests.exceptions.RequestException as e:
        logger.error(f"Lark 消息发送请求失败 (网络/超时): {e}. 标题: {title}")
    except Exception as e:
        logger.error(f"发送 Lark 告警时发生未知错误: {e}. 标题: {title}")