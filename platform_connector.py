import requests
import logging
import time
from datetime import datetime

# 获取 logger 实例，与主脚本使用相同的日志配置
logger = logging.getLogger('monitor_system')

# K线数据的索引映射（标准 OHLCV 格式）
# 确保与你 MySQL 写入逻辑中的索引对应
OHLCV_INDEX = {
    'timestamp': 0,
    'open': 1,
    'high': 2,
    'low': 3,
    'close': 4,
    'volume': 5,
    'quote_volume': 6
}

# 假设 K线 API 路径，请根据你的 BITDA 实际 API 路径调整
# 示例：如果是 /api/v3/klines，那么 API 完整 URL 是 api_base_url + KLINE_API_PATH
KLINE_API_PATH = "/api/v2/klines"


class PlatformConnector:
    """
    交易所数据连接器，负责处理 API 请求和数据转换。
    此版本适用于不支持 limit 参数的 API，并包含重试机制。
    """

    def __init__(self, platform_id, api_base_url):
        self.platform_id = platform_id
        self.api_base_url = api_base_url
        logger.info(f"Connector initialized for {platform_id} @ {api_base_url}")

    def _convert_symbol(self, symbol):
        """将标准格式 (如 BTC/USDT) 转换为交易所要求的格式 (如 BTCUSDT)。"""
        # 🚨 请根据 BITDA 的实际要求调整这里的转换逻辑
        return symbol.replace('/', '')

    def fetch_ohlcv_history(self, symbol, timeframe, start_time_ms=None):

        exchange_symbol = self._convert_symbol(symbol)
        params = {}

        # 1. 核心逻辑：根据平台 ID 确定固定的 URL 和可变的参数名
        if self.platform_id == 'BITDA_FUTURES':
            # 🚨 固定 URL 路径
            url = f"{self.api_base_url}/open/api/v2/market/kline"

            # 🚨 固定参数名和参数值转换
            params['market'] = exchange_symbol  # 标的参数名
            params['type'] = '1min' if timeframe == '1m' else timeframe  # 周期参数名和值

        elif self.platform_id == 'BINANCE_FUTURES':
            # 🚨 固定 URL 路径 (请确保这个路径是正确的)
            url = f"{self.api_base_url}/fapi/v1/klines"

            # 🚨 固定参数名
            params['symbol'] = exchange_symbol
            params['interval'] = timeframe

        else:
            logger.error(f"不支持的交易所 ID: {self.platform_id}. 无法构造 API 请求。")
            return None

        # 2. 统一添加 start_time_ms 参数
        if start_time_ms:
            params['startTime'] = start_time_ms

        # --- 3. 发送请求 (新增重试机制) ---
        max_retries = 3
        retry_delay_seconds = 2  # 初始等待时间

        response = None
        status_code = None  # 初始化 status_code

        for attempt in range(max_retries):
            try:
                # 尝试请求
                response = requests.get(url, params=params, timeout=10)

                # 如果成功，获取状态码并检查 4xx/5xx
                status_code = response.status_code
                response.raise_for_status()

                # 如果成功 (状态码 200)，退出重试循环
                break

            except requests.exceptions.HTTPError as e:
                # 捕获 4xx/5xx 错误（由 raise_for_status 抛出）
                # 此时 status_code 已经被正确赋值为 int

                # 🚨 修复 1: 确保 status_code 是 int 才能比较
                if isinstance(status_code, int):
                    # 判定是否为不可重试的错误 (例如 401/403，排除 429)
                    is_unrecoverable_4xx = status_code >= 400 and status_code < 500 and status_code != 429
                else:
                    # 理论上 HTTPError 应该伴随 status_code，以防万一
                    is_unrecoverable_4xx = True

                if attempt == max_retries - 1 or is_unrecoverable_4xx:
                    # 记录最终错误并返回 None
                    logger.error(f"[{self.platform_id}][{symbol}] 请求 API 最终失败 (Code: {status_code}): {e}")
                    return None

                # 针对可重试的 5xx 或 429 错误，进行等待和重试
                logger.warning(
                    f"[{self.platform_id}][{symbol}] 请求 API 失败 (Code: {status_code})，"
                    f"将在 {retry_delay_seconds} 秒后重试 (尝试 {attempt + 1}/{max_retries})."
                )
                time.sleep(retry_delay_seconds)
                retry_delay_seconds *= 2  # 指数退避 (2, 4, 8 秒)

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                # 捕获连接错误或超时，此时 response 是 None 或不完整，status_code 保持 None

                if attempt == max_retries - 1:
                    logger.error(f"[{self.platform_id}][{symbol}] 请求 API 最终失败 (网络/超时): {e}")
                    return None

                logger.warning(
                    f"[{self.platform_id}][{symbol}] 请求 API 失败 (网络/超时)，"
                    f"将在 {retry_delay_seconds} 秒后重试 (尝试 {attempt + 1}/{max_retries})."
                )
                time.sleep(retry_delay_seconds)
                retry_delay_seconds *= 2  # 指数退避

            except requests.exceptions.RequestException as e:
                # 捕获所有其他 requests 异常（如 TooManyRedirects）
                logger.error(f"[{self.platform_id}][{symbol}] 请求 API 发生未知 RequestException: {e}")
                return None


        else:
            # 如果循环结束仍未 break (表示所有重试都失败了)
            logger.error(f"[{self.platform_id}][{symbol}] 超过最大重试次数，请求失败。")
            return None

        # --- 4. 数据解析 (位于重试循环之后) ---
        try:
            data = response.json()

            # 统一的数据封装/错误处理逻辑
            if isinstance(data, dict) and 'code' in data:
                if data['code'] != 0:
                    logger.error(f"[{self.platform_id}][{symbol}] API 业务错误: Code={data['code']}, Msg={data['msg']}")
                    return None
                else:
                    # BITDA 风格：Code=0, 成功数据在 data 字段中
                    if 'data' in data and isinstance(data['data'], list):
                        return data['data']
                    else:
                        return []  # 避免 BITDA 成功但数据为空的情况

            # 兼容 Binance 风格：成功时直接返回 K线列表
            if isinstance(data, list):
                return data
            else:
                logger.warning(f"[{self.platform_id}][{symbol}] API 返回数据格式不符合预期。完整响应: {data}")
                return None

        except Exception as e:
            logger.error(f"[{self.platform_id}][{symbol}] 处理 API 数据时发生未知错误: {e}")
            return None