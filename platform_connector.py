# platform_connector.py
import time
import logging
from datetime import datetime
from typing import List, Optional, Any, Dict

import requests

logger = logging.getLogger('monitor_system')


def _fmt_ms(ms: Optional[int]) -> str:
    if ms is None:
        return "None"
    try:
        return f"{ms} ({datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S')})"
    except Exception:
        return str(ms)


def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


class PlatformConnector:
    """
    统一接口：
      fetch_ohlcv_history(symbol, timeframe, start_time_ms=None) -> [[ts_ms, open, high, low, close, volume], ...]
    """

    # timeframe 映射
    _TF_MAP_BITDA = {
        "1m": "1min", "3m": "3min", "5m": "5min",
        "15m": "15min", "30m": "30min",
        "1h": "1hour", "4h": "4hour", "6h": "6hour", "12h": "12hour",
        "1d": "1day", "1w": "1week"
    }
    _TF_MAP_BINANCE = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
        "1d": "1d", "1w": "1w"
    }

    # timeframe → 秒（便于计算 end_time）
    _TF_TO_SECONDS = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
        "1d": 86400, "1w": 604800
    }

    # 如需符号映射（某平台命名不同），可在此配置
    _SYMBOL_MAP: Dict[str, Dict[str, str]] = {
        # "BITDA_FUTURES": {"ETHUSDT": "ETH_USDT"},
        # "BINANCE_FUTURES": {}
    }

    # 为 BITDA 计算 end_time 时使用的窗口大小（根数）
    _BITDA_WINDOW_CANDLES = 200  # 可按需调整

    def __init__(self, platform_id: str, base_url: str, timeout: int = 10):
        self.platform_id = platform_id.upper().strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        # 调试/诊断字段
        self.last_request: Dict[str, Any] = {}
        self.last_response_status: Optional[int] = None
        self.last_response_text: Optional[str] = None
        self.last_error: Optional[str] = None

        # 简单限速（避免 1r/s 限速触发）
        self._last_call_ts: float = 0.0
        self._min_interval_sec: float = 1.05  # 保守 >1s

    # ======= 公共入口 =======
    def fetch_ohlcv_history(self, symbol: str, timeframe: str, start_time_ms: Optional[int] = None) -> Optional[List[list]]:
        # 限速
        self._throttle()

        # 清理调试字段
        self.last_request = {}
        self.last_response_status = None
        self.last_response_text = None
        self.last_error = None

        # 符号映射
        symbol_real = self._SYMBOL_MAP.get(self.platform_id, {}).get(symbol, symbol)

        try:
            if self.platform_id.startswith("BINANCE"):
                data = self._fetch_binance(symbol_real, timeframe, start_time_ms)
            elif self.platform_id.startswith("BITDA"):
                data = self._fetch_bitda(symbol_real, timeframe, start_time_ms)
            else:
                raise NotImplementedError(f"未知平台: {self.platform_id}")
            return data
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            logger.error(f"[{self.platform_id}] fetch_ohlcv_history 失败：{self.last_error} | req={self.last_request}", exc_info=True)
            return None
        finally:
            self._last_call_ts = time.time()

    # ======= 平台实现：BINANCE（期货）=======
    def _fetch_binance(self, symbol: str, timeframe: str, start_time_ms: Optional[int]) -> List[list]:
        interval = self._TF_MAP_BINANCE.get(timeframe, timeframe)
        url = f"{self.base_url}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)

        self.last_request = {
            "platform": self.platform_id, "url": url, "params": params,
        }

        resp = self.session.get(url, params=params, timeout=self.timeout)
        self.last_response_status = resp.status_code
        self.last_response_text = resp.text[:5000]
        resp.raise_for_status()

        data = resp.json()
        out = []
        for item in data:
            if not isinstance(item, list) or len(item) < 6:
                continue
            ts_ms = int(item[0])
            o = _safe_float(item[1]); h = _safe_float(item[2]); l = _safe_float(item[3]); c = _safe_float(item[4]); v = _safe_float(item[5])
            out.append([ts_ms, o, h, l, c, v])
        return out

    # ======= 平台实现：BITDA =======
    def _fetch_bitda(self, symbol: str, timeframe: str, start_time_ms: Optional[int]) -> List[list]:
        tf_std = timeframe.strip().lower()
        tf_bitda = self._TF_MAP_BITDA.get(tf_std, tf_std)
        tf_sec = self._TF_TO_SECONDS.get(tf_std, 60)  # 默认按1m

        url = f"{self.base_url}/open/api/v2/market/kline"

        def _do_request(use_start: bool) -> List[list]:
            params = {"market": symbol, "type": tf_bitda}
            if use_start and start_time_ms is not None:
                start_sec = int(start_time_ms // 1000)
                # 计算 end_time：窗口若干根，且不能超过“当前 - 1个周期”
                now_sec = int(time.time())
                end_sec = start_sec + self._BITDA_WINDOW_CANDLES * tf_sec
                end_sec = min(end_sec, now_sec - tf_sec)  # BITDA 有些实现要求 end < now
                if end_sec <= start_sec:
                    end_sec = start_sec + tf_sec  # 至少一个周期
                params["start_time"] = start_sec
                params["end_time"] = end_sec

            self.last_request = {
                "platform": self.platform_id,
                "url": url,
                "params": params,
                "start_time_str": _fmt_ms(start_time_ms) if use_start and start_time_ms is not None else None,
            }

            resp = self.session.get(url, params=params, timeout=self.timeout)
            self.last_response_status = resp.status_code
            self.last_response_text = resp.text[:5000]
            resp.raise_for_status()

            j = resp.json()
            code = j.get("code")
            # code 为 0 或不返回 code 视为成功
            if code not in (0, "0", None):
                # 将错误抛出，外层做回退
                raise RuntimeError(f"BITDA API 返回错误 code={code}, msg={j.get('msg')}")

            raw = j.get("data", [])
            items = self._flatten_bitda_data(raw)
            out = []
            for obj in items:
                if isinstance(obj, dict):
                    ts_ms = int(obj.get("time"))
                    o = _safe_float(obj.get("open")); h = _safe_float(obj.get("high")); l = _safe_float(obj.get("low"))
                    c = _safe_float(obj.get("close")); v = _safe_float(obj.get("volume"))
                elif isinstance(obj, list) and len(obj) >= 6:
                    ts_ms = int(obj[0]); o = _safe_float(obj[1]); h = _safe_float(obj[2])
                    l = _safe_float(obj[3]); c = _safe_float(obj[4]); v = _safe_float(obj[5])
                else:
                    continue
                out.append([ts_ms, o, h, l, c, v])
            return out

        # 先尝试带 start/end（更高效的增量）
        try:
            data = _do_request(use_start=True)
            if data:
                return data
        except RuntimeError as e:
            # 如果是 time interval invalid（code=10014），回退到“最近窗口（不带 start/end）”
            if "10014" in str(e) or "interval invalid" in str(e).lower():
                logger.warning(f"[{self.platform_id}] BITDA 带 start/end 失败（{e}），回退为最近窗口拉取。")
            else:
                # 其他错误也回退尝试一次
                logger.warning(f"[{self.platform_id}] BITDA 带 start/end 失败（{e}），尝试最近窗口拉取。")
        except Exception as e:
            logger.warning(f"[{self.platform_id}] BITDA 带 start/end 请求异常（{e}），尝试最近窗口拉取。")

        # 回退：不带 start/end，取最近窗口（由交易所默认 limit 决定）
        try:
            data = _do_request(use_start=False)
            return data
        except Exception as e:
            # 回退仍失败，抛给上层记录
            raise

    @staticmethod
    def _flatten_bitda_data(raw: Any) -> List[Any]:
        """
        尽量把 BITDA 的 data 解成一维列表（元素为字典/数组）。
        支持：
          - [ [ {...}, {...} ] ]
          - [ {...}, {...} ]
          - []
        """
        if not isinstance(raw, list):
            return []
        if raw and all(isinstance(x, list) for x in raw):
            flat = []
            for sub in raw:
                flat.extend(sub if isinstance(sub, list) else [])
            return flat
        return raw

    # ======= 内部：简单限速 =======
    def _throttle(self):
        now = time.time()
        delta = now - self._last_call_ts
        if delta < self._min_interval_sec:
            time.sleep(self._min_interval_sec - delta)
