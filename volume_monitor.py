# volume_monitor.py
import time
import logging
from datetime import datetime
import pymysql
import sys
import os
from typing import Union, Tuple, Dict, Set, List

from utils import load_config, init_db

try:
    from lark_alerter import send_lark_alert
except ImportError:
    def send_lark_alert(*args, **kwargs):
        logging.getLogger('monitor_system').error("Lark告警模块未找到，无法发送通知。")

logger = logging.getLogger('monitor_system')

# -------------------- 去重/冷却缓存 --------------------
# 同一 symbol 的同一“窗口结束时间”只比对/报警一次
_LAST_CHECKED_END_TS: Dict[str, Union[int, float, datetime]] = {}
_LAST_ALERTED_END_TS: Dict[str, Union[int, float, datetime]] = {}
# 冷却：记录最后一次真实报警的 wall-clock（秒）
_LAST_ALERT_WALLCLOCK: Dict[str, int] = {}

# -------------------- 日志 --------------------
def setup_logging():
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); ch.setLevel(logging.INFO)
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'volume_monitor_log.log')
    fh = logging.FileHandler(log_file, encoding='utf-8'); fh.setFormatter(fmt); fh.setLevel(logging.INFO)
    root_logger = logging.getLogger('monitor_system')
    root_logger.addHandler(ch); root_logger.addHandler(fh)
    root_logger.setLevel(logging.INFO)

# -------------------- 工具 --------------------
def format_timestamp(ts: Union[int, float, datetime, None]) -> str:
    if ts is None:
        return "N/A"
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, (int, float)):
        # 认为是毫秒时间戳
        dt = datetime.fromtimestamp(float(ts) / 1000.0)
    else:
        logger.error(f"format_timestamp 收到不支持的类型: {type(ts)}")
        return "类型错误"
    return dt.strftime('%Y-%m-%d %H:%M')

def _ci_get(d: dict, key: str):
    if not isinstance(d, dict): return None, None
    t = key.strip().lower()
    for k, v in d.items():
        if isinstance(k, str) and k.strip().lower() == t:
            return v, k
    return None, None

def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _to_epoch_ms(x: Union[int, float, datetime, None]) -> Union[int, None]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return int(x.timestamp() * 1000)
    try:
        return int(x)
    except Exception:
        return None

# -------------------- 读取阈值/窗口（全局 + 按币种覆盖） --------------------
def _read_volume_params(config: dict, symbol: str) -> Tuple[float, float, int, int]:
    """
    返回：
      target_ratio: float             (默认 0.20)
      tolerance:    float             (默认 0.20)
      cooldown_min: int               (默认 0)
      window_len:   int (candles)     (默认 15，可被 SYMBOL_THRESHOLDS 覆盖)
    """
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}

    # 目标系数
    sym_tr_val, _ = _ci_get(sym_conf, 'VOLUME_TARGET_RATIO')
    glb_tr_val, _ = _ci_get(ac,       'VOLUME_TARGET_RATIO')
    target_ratio = sym_tr_val if sym_tr_val is not None else (glb_tr_val if glb_tr_val is not None else 0.20)

    # 偏离容差
    sym_tol_val, _ = _ci_get(sym_conf, 'VOLUME_RATIO_THRESHOLD')
    glb_tol_val, _ = _ci_get(ac,       'VOLUME_RATIO_THRESHOLD')
    tolerance = sym_tol_val if sym_tol_val is not None else (glb_tol_val if glb_tol_val is not None else 0.20)

    # 冷却（分钟）
    cooldown_min = int(ac.get('ALERT_COOLDOWN_MINUTES', 0))

    # 窗口（根数）
    sym_win = sym_conf.get('VOLUME_COMPARE_WINDOW')
    glb_win = ac.get('VOLUME_COMPARE_WINDOW', 15)
    window_len = sym_win if sym_win is not None else glb_win
    try:
        window_len = int(window_len)
        if window_len < 1:
            window_len = 15
    except Exception:
        window_len = 15

    try: target_ratio = float(target_ratio)
    except Exception: target_ratio = 0.20
    try: tolerance = float(tolerance)
    except Exception: tolerance = 0.20

    logger.info(f"[{symbol}] 成交量阈值：target_ratio={target_ratio:.2f} | tolerance={tolerance:.2f} | window={window_len} | cooldown={cooldown_min}m")
    return target_ratio, tolerance, cooldown_min, window_len

def _cooldown_ok(symbol: str, cooldown_min: int) -> bool:
    """基于 wall-clock 的冷却判断。"""
    if cooldown_min <= 0:
        return True
    now_s = int(time.time())
    last_s = _LAST_ALERT_WALLCLOCK.get(symbol)
    if last_s is None:
        return True
    return (now_s - last_s) >= cooldown_min * 60

# -------------------- DB 读取 --------------------
def _select_recent_volume_rows(cursor, table: str, symbol: str, exchange: str, limit: int) -> List[dict]:
    sql = f"""
        SELECT timestamp, volume
        FROM {table}
        WHERE symbol = %s AND exchange = %s
        ORDER BY timestamp DESC
        LIMIT {int(limit)}
    """
    cursor.execute(sql, (symbol, exchange))
    return cursor.fetchall() or []

def _pick_latest_common_timestamps(rows_a: List[dict], rows_b: List[dict], need: int) -> List[Union[int, float, datetime]]:
    if not rows_a or not rows_b:
        return []
    set_a = {r['timestamp'] for r in rows_a}
    set_b = {r['timestamp'] for r in rows_b}
    commons = sorted(list(set_a & set_b))
    if len(commons) < need:
        return []
    return commons[-need:]

def _sum_vol_on_timestamps(rows: List[dict], ts_keep: Set[Union[int, float, datetime]]) -> float:
    if not rows or not ts_keep:
        return 0.0
    s = 0.0
    for r in rows:
        if r['timestamp'] in ts_keep:
            s += _to_float(r['volume'])
    return s

# -------------------- 主逻辑 --------------------
def compare_volume_alert(conn, config):
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    if not bool(ac.get('VOLUME_ALERT_ENABLED', True)):
        logger.info("成交量对比已关闭（ALERT_CONFIG.VOLUME_ALERT_ENABLED=false），跳过。")
        return

    symbols = (
        (config or {}).get('MONITORED_SYMBOLS')
        or (config.get('EXCHANGE_CONFIG') or {}).get('MONITORED_SYMBOLS')
        or []
    )
    if not symbols:
        logger.critical("配置缺少顶层 MONITORED_SYMBOLS（或为空）。请在 config.toml 顶层添加 MONITORED_SYMBOLS = [\"BTCUSDT\", ...]")
        return

    ex_conf = config['EXCHANGE_CONFIG']
    table_names = config['TABLE_NAMES']
    lark_app_config = config['LARK_APP_CONFIG']

    A_ID = ex_conf['PLATFORM_A_ID'].upper()
    B_ID = ex_conf['BENCHMARK_ID'].upper()
    timeframe = (ex_conf.get('TIME_FRAME') or '1m').lower()

    # TIME_FRAME → 秒（用于“期望末尾”诊断）
    tf_map_sec = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,"4h":14400,"6h":21600,"8h":28800,"12h":43200,"1d":86400,"1w":604800}
    tf_sec = tf_map_sec.get(timeframe, 60)

    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    logger.info("=" * 76)
    logger.info(f"📢 成交量对比开始：A={A_ID} vs B={B_ID} | TIME_FRAME={timeframe}")

    for symbol in symbols:
        try:
            # 读取阈值 + 窗口
            target_ratio, tolerance, cooldown_min, window_len = _read_volume_params(config, symbol)
            min_common = window_len
            # 多取一些给对齐留冗余
            fetch_n = max(window_len * 2, window_len + 5)

            rows_a = _select_recent_volume_rows(cursor, table_names['KLINE_DATA'], symbol, A_ID, fetch_n)
            rows_b = _select_recent_volume_rows(cursor, table_names['KLINE_DATA'], symbol, B_ID, fetch_n)

            a_latest = rows_a[0]['timestamp'] if rows_a else None
            b_latest = rows_b[0]['timestamp'] if rows_b else None
            now_s = int(time.time())
            expected_end_s = (now_s // tf_sec) * tf_sec
            expected_end_str = datetime.fromtimestamp(expected_end_s).strftime('%Y-%m-%d %H:%M')

            logger.info(
                f"[{symbol}] A最新: {format_timestamp(a_latest)} | "
                f"B最新: {format_timestamp(b_latest)} | "
                f"期望末尾: {expected_end_str}"
            )

            if len(rows_a) < window_len or len(rows_b) < window_len:
                latest_any = a_latest or b_latest
                logger.warning(f"[{format_timestamp(latest_any)}][{symbol}] 数据不足：A({len(rows_a)}), B({len(rows_b)}), 需≥{window_len}，跳过。")
                continue

            commons = _pick_latest_common_timestamps(rows_a, rows_b, window_len)
            if not commons or len(commons) < min_common:
                latest_any = a_latest or b_latest
                logger.warning(f"[{format_timestamp(latest_any)}][{symbol}] A/B 最近窗口无足够共同时间戳（需 {window_len}），跳过。")
                continue

            common_end = commons[-1]
            common_end_ms = _to_epoch_ms(common_end)
            lag_sec = max(0, expected_end_s - int((common_end_ms or 0)//1000))
            lag_min = lag_sec // 60
            logger.info(
                f"[{symbol}] 共同末尾: {format_timestamp(common_end)} | 距期望末尾落后: {lag_min} 分钟"
            )

            ts_start, ts_end = commons[0], commons[-1]
            formatted_time_start = format_timestamp(ts_start)
            formatted_time_end = format_timestamp(ts_end)

            # 去重：同一 symbol 的同一 end_ts 不重复比对
            if _LAST_CHECKED_END_TS.get(symbol) == ts_end:
                logger.info(f"[{symbol}] 已处理过窗口（结束 {formatted_time_end}），跳过重复计算。")
                continue
            _LAST_CHECKED_END_TS[symbol] = ts_end

            keep = set(commons)
            A_sum = _sum_vol_on_timestamps(rows_a, keep)
            B_sum = _sum_vol_on_timestamps(rows_b, keep)

            logger.info(f"[{formatted_time_end}][{symbol}] ✅ 数据对齐成功。共同 K 线数量: {len(keep)}（窗口={window_len}）。")

            target_val = target_ratio * B_sum
            if B_sum == 0 or target_val == 0:
                within = (A_sum == 0.0)
                rel_str = "0.00%" if within else "Inf"
            else:
                rel = (A_sum - target_val) / target_val
                within = abs(rel) <= tolerance
                rel_str = f"{rel:+.2%}"

            # 输出明细
            logger.info("-" * 74)
            logger.info(f"--- 📊 {formatted_time_start} -> {formatted_time_end} | {symbol} 成交量对比（窗口 {window_len} 根） ---")
            logger.info(f"规则：A ≈ {target_ratio:.2f} × B，允许偏差 ±{tolerance:.0%}（相对目标值）")
            logger.info(f"A({A_ID}) 累计: {A_sum:.2f} | B({B_ID}) 累计: {B_sum:.2f} | 目标={target_val:.2f} | 偏差={rel_str}")

            # 报警（同一 end_ts 不重复 + 冷却）
            if within:
                logger.info(f"[{symbol}] ✅ 正常（窗口结束 {formatted_time_end}）")
            else:
                if _LAST_ALERTED_END_TS.get(symbol) == ts_end:
                    logger.info(f"[{symbol}] 已对该窗口报警过（结束 {formatted_time_end}），跳过重复推送。")
                else:
                    if _cooldown_ok(symbol, cooldown_min):
                        title = f"🚨 成交量偏离阈值: {symbol} @ {formatted_time_end} ({window_len}m 累计)"
                        text = (
                            f"**时间范围**: {formatted_time_start} -> {formatted_time_end}\n"
                            f"A({A_ID})={A_sum:.2f}\n"
                            f"B({B_ID})={B_sum:.2f}\n"
                            f"目标 = {target_ratio:.2f} × B = {target_val:.2f}\n"
                            f"相对偏差 = {rel_str}\n"
                            f"容差 = ±{tolerance:.0%}"
                        )
                        send_lark_alert(lark_app_config, title, text)
                        _LAST_ALERTED_END_TS[symbol] = ts_end
                        _LAST_ALERT_WALLCLOCK[symbol] = int(time.time())
                    else:
                        logger.info(f"[{symbol}] 报警处于冷却期，跳过发送（窗口结束 {formatted_time_end}）。")

            logger.info("-" * 74)

        except pymysql.Error as db_err:
            logger.error(f"[{symbol}] 数据库操作失败: {db_err}", exc_info=True)
            conn.rollback()
        except Exception as e:
            logger.critical(f"[{symbol}] 成交量对比发生未知异常: {e}", exc_info=True)

    cursor.close()
    logger.info("🎉 本轮成交量对比完成。")
    logger.info("=" * 76)

def main():
    setup_logging()
    conn = None
    try:
        config = load_config()
        conn = init_db(config)

        # >>>>>>> 关键新增：读到最新数据 <<<<<<<
        try:
            conn.autocommit(True)
        except Exception:
            pass
        try:
            with conn.cursor() as c:
                c.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception as _:
            logger.warning("设置 READ COMMITTED 失败，使用默认隔离级别继续。")

        frequency = int((config.get('EXCHANGE_CONFIG') or {}).get('FREQUENCY_SECONDS', 60))
        logger.info(f"成交量监控脚本已启动，运行频率为每 {frequency} 秒一次...")

        while True:
            # 保证连接可用
            try:
                conn.ping(reconnect=True)
            except Exception:
                logger.warning("数据库连接失效，尝试重新建立连接…")
                conn = init_db(config)
                try:
                    conn.autocommit(True)
                    with conn.cursor() as c:
                        c.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                except Exception:
                    pass

            # 热加载配置并比对
            config = load_config()
            compare_volume_alert(conn, config)
            time.sleep(frequency)

    except KeyboardInterrupt:
        logger.info("用户中断程序 (Ctrl+C)。程序正在退出...")
    except Exception as e:
        logger.critical(f"交易量脚本发生致命错误，正在退出: {e}", exc_info=True)
    finally:
        if conn:
            conn.close(); logger.info("数据库连接已关闭。程序退出。")


if __name__ == '__main__':
    main()
