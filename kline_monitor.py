# kline_monitor.py
import time
import logging
from datetime import datetime
import pymysql
import sys
import os
from typing import Union, Dict, Tuple, List, Optional

from utils import load_config, init_db
from platform_connector import PlatformConnector

# ====== 新增：改用 Teams 邮件通道 ======
try:
    from teams_alerter import send_teams_alert
except ImportError:
    def send_teams_alert(*args, **kwargs):
        logging.getLogger('monitor_system').error("teams_alerter 未找到，无法发送通知。")

logger = logging.getLogger('monitor_system')

# ================== 常量（可按需上移到 config） ==================
RECENT_LIMIT = 20          # 回补数量（空表/过旧或失败时回补最近N根）
STALE_LIMIT_DAYS = 2       # 数据过旧阈值（天）
STALE_LIMIT_MS = STALE_LIMIT_DAYS * 24 * 60 * 60 * 1000

# === 表的 timestamp 列类型：True=DATETIME，False=BIGINT(毫秒) ===
TIMESTAMP_IS_DATETIME = True

# ================== 日志 ==================
def setup_logging():
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); ch.setLevel(logging.INFO)
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kline_monitor_log.log')
    fh = logging.FileHandler(log_file, encoding='utf-8'); fh.setFormatter(fmt); fh.setLevel(logging.INFO)
    root_logger = logging.getLogger('monitor_system')
    root_logger.addHandler(ch); root_logger.addHandler(fh)
    root_logger.setLevel(logging.INFO)

# ================== 统一通知封装（以后要并发发 Lark/Teams，只需改这里） ==================
def notify(config: dict, title: str, text: str, *, dedup_key: str = None) -> None:
    """
    统一出口：当前使用 Teams 频道邮箱发送。
    - dedup_key: 同一键在 teams_alerter 内部窗口期仅发一次（避免重复）
    """
    send_teams_alert(config, title, text, dedup_key=dedup_key)

# ================== 小工具 ==================
def now_ms() -> int:
    return int(time.time() * 1000)

def format_timestamp(ts: Union[int, float, datetime, None]) -> str:
    if ts is None: return "N/A"
    if isinstance(ts, datetime): dt = ts
    elif isinstance(ts, (int, float)): dt = datetime.fromtimestamp(ts / 1000.0)
    else: logger.error(f"format_timestamp 类型错误: {type(ts)}"); return "类型错误"
    return dt.strftime('%Y-%m-%d %H:%M')

def _ci_get(d: dict, key: str):
    """大小写不敏感取值，返回 (value, matched_key)"""
    if not isinstance(d, dict): return None, None
    t = key.strip().lower()
    for k, v in d.items():
        if isinstance(k, str) and k.strip().lower() == t:
            return v, k
    return None, None

def _get_symbols(cfg: dict) -> List[str]:
    syms = (
        (cfg or {}).get('MONITORED_SYMBOLS')
        or (cfg.get('EXCHANGE_CONFIG') or {}).get('MONITORED_SYMBOLS')
        or []
    )
    if not syms:
        logger.critical('配置缺少顶层 MONITORED_SYMBOLS（或为空）。请在 config.toml 顶层添加 MONITORED_SYMBOLS = ["BTCUSDT", ...]')
    return syms

# === timeframe 解析为毫秒 ===
_TIMEFRAME_TO_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000
}
def _interval_ms_from_timeframe(tf: str) -> int:
    tf = (tf or "1m").strip().lower()
    return _TIMEFRAME_TO_MS.get(tf, 60_000)

def _normalize_candle_start(ts_ms: int, interval_ms: int) -> int:
    """把任意时间戳规整到它所在K线的开盘时刻（向下取整到粒度边界）。"""
    if ts_ms is None: return None
    return (int(ts_ms) // interval_ms) * interval_ms

# ================== 阈值读取 ==================
def _read_price_thresholds(config: dict, symbol: str) -> Tuple[bool, float, float, float, float]:
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}

    enabled = bool(ac.get('KLINE_PRICE_ALERT_ENABLED', True))

    def pick(key: str, default: float) -> float:
        sv, _ = _ci_get(sym_conf, key)
        gv, _ = _ci_get(ac, key)
        val = sv if sv is not None else (gv if gv is not None else default)
        try: return float(val)
        except Exception: return default

    th_open  = pick('OPEN_DEVIATION_THRESHOLD',  0.002)
    th_high  = pick('HIGH_DEVIATION_THRESHOLD',  0.001)
    th_low   = pick('LOW_DEVIATION_THRESHOLD',   0.001)
    th_close = pick('CLOSE_DEVIATION_THRESHOLD', 0.0005)

    logger.info(
        f"[{symbol}] 价格阈值：ENABLED={enabled} | "
        f"OPEN={th_open:.4%}, HIGH={th_high:.4%}, LOW={th_low:.4%}, CLOSE={th_close:.4%}"
    )
    return enabled, th_open, th_high, th_low, th_close

def _read_one_line_thresholds(config: dict, symbol: str):
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym = (ac.get('SYMBOL_THRESHOLDS') or {}).get(symbol, {}) or {}
    enabled         = bool(ac.get('ONE_LINE_KLINE_ALERT_ENABLED', True))
    count_threshold = int(sym.get('ONE_LINE_KLINE_COUNT', ac.get('ONE_LINE_KLINE_COUNT', 4)))
    epsilon         = float(sym.get('ONE_LINE_EPSILON', ac.get('ONE_LINE_EPSILON', 0.0)))
    require_streak  = int(ac.get('ALERT_REQUIRE_STREAK', 1))
    cooldown_min    = int(ac.get('ALERT_COOLDOWN_MINUTES', 0))
    logger.info(f"[{symbol}] 一字线阈值：enabled={enabled} count={count_threshold} eps={epsilon} streak={require_streak} cooldown={cooldown_min}m")
    return enabled, count_threshold, epsilon, require_streak, cooldown_min

def _is_one_line(o: float, h: float, l: float, c: float, eps: float) -> bool:
    return (abs(h-l) <= eps and abs(o-c) <= eps and abs(o-h) <= eps and abs(c-l) <= eps)

# ================== DB 读写（DATETIME/BIGINT 兼容） ==================
def _get_latest_ts(cursor, table: str, symbol: str, exchange: str) -> Optional[int]:
    if TIMESTAMP_IS_DATETIME:
        sql = f"SELECT MAX(UNIX_TIMESTAMP(`timestamp`)*1000) AS ts_ms FROM {table} WHERE symbol=%s AND exchange=%s"
        cursor.execute(sql, (symbol, exchange))
        row = cursor.fetchone() or {}
        ts = row.get('ts_ms')
        return int(ts) if ts is not None else None
    else:
        sql = f"SELECT MAX(`timestamp`) AS ts FROM {table} WHERE symbol=%s AND exchange=%s"
        cursor.execute(sql, (symbol, exchange))
        row = cursor.fetchone() or {}
        ts = row.get('ts')
        return int(ts) if ts is not None else None

def _upsert_klines(conn, cursor, table: str, exchange: str, symbol: str, rows: List[list], interval_ms: int):
    """
    rows: 每条 K 线至少包含 [ts_ms, open, high, low, close, volume]
    入库前会将 ts 规整到 K 线“开盘时刻”。
    需要表有唯一键 (exchange, symbol, timestamp) 以便去重。
    """
    if not rows:
        return

    # 规整 ts 到开盘边界
    normed = []
    for r in rows:
        try:
            ts_ms = int(r[0])
            ts_ms = _normalize_candle_start(ts_ms, interval_ms)
            o, h, l, c, v = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
            normed.append((ts_ms, o, h, l, c, v))
        except Exception:
            continue
    if not normed:
        return

    if TIMESTAMP_IS_DATETIME:
        sql = f"""
            INSERT INTO {table} 
            (exchange, symbol, `timestamp`, open, high, low, close, volume)
            VALUES (%s,%s,FROM_UNIXTIME(%s/1000),%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume)
        """
        data = [(exchange, symbol, ts, o, h, l, c, v) for (ts, o, h, l, c, v) in normed]
    else:
        sql = f"""
            INSERT INTO {table} 
            (exchange, symbol, `timestamp`, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume)
        """
        data = [(exchange, symbol, ts, o, h, l, c, v) for (ts, o, h, l, c, v) in normed]

    cursor.executemany(sql, data)
    conn.commit()
    logger.info(f"[{symbol}] {exchange} 入库/更新 {len(data)} 条 K 线。")

def _select_recent_rows(cursor, table: str, symbol: str, exchange: str, limit: int, interval_ms: int) -> List[dict]:
    """读取最近 limit 条，统一返回 ts_ms 字段（且再做一次规整，以防历史数据里混入未规整 ts）"""
    if TIMESTAMP_IS_DATETIME:
        sql = f"""
            SELECT UNIX_TIMESTAMP(`timestamp`)*1000 AS ts_ms, open, high, low, close
            FROM {table}
            WHERE symbol = %s AND exchange = %s
            ORDER BY `timestamp` DESC
            LIMIT {int(limit)}
        """
    else:
        sql = f"""
            SELECT `timestamp` AS ts_ms, open, high, low, close
            FROM {table}
            WHERE symbol = %s AND exchange = %s
            ORDER BY `timestamp` DESC
            LIMIT {int(limit)}
        """
    cursor.execute(sql, (symbol, exchange))
    rows = cursor.fetchall() or []
    # 再规整一次，避免历史数据中有未规整 ts（只在内存中规整用于对齐）
    for r in rows:
        r['ts_ms'] = _normalize_candle_start(int(r['ts_ms']), interval_ms)
    return rows

# ================== 拉取 → 入库（每轮执行） ==================
def _ingest_latest_for(conn, cursor, table: str, connector: PlatformConnector,
                       exchange_id: str, symbol: str, timeframe: str, interval_ms: int):
    """
    逻辑：
    - 空表/过旧：从 now-RECENT_LIMIT*interval 回补；
    - 正常：从 latest_ts+interval 增量拉取；
    - 若本次 fetch 为空，则退化为“最近 RECENT_LIMIT 根”（不带 start）再试一次。
    """
    latest_ts = _get_latest_ts(cursor, table, symbol, exchange_id)
    now = now_ms()

    if latest_ts is None:
        start_ms = now - RECENT_LIMIT * interval_ms
        logger.info(f"[{symbol}] {exchange_id} 空表，回补最近 {RECENT_LIMIT} 根（起: {format_timestamp(start_ms)}）。")
    else:
        age = now - latest_ts
        if age > STALE_LIMIT_MS:
            start_ms = now - RECENT_LIMIT * interval_ms
            logger.warning(f"[{symbol}] {exchange_id} 最新数据过旧（{format_timestamp(latest_ts)}），回补最近 {RECENT_LIMIT} 根。")
        else:
            start_ms = latest_ts + interval_ms
            logger.info(f"[{symbol}] {exchange_id} 增量拉取，自 {format_timestamp(start_ms)} 起。")

    # 1) 带 start 的拉取
    klines = connector.fetch_ohlcv_history(symbol, timeframe, start_time_ms=start_ms)
    if klines is None:
        logger.error(f"[{symbol}] {exchange_id} 拉取失败（带 start）；")
        klines = []
    logger.info(f"[{symbol}] {exchange_id} fetch(带start) 返回 {len(klines)} 条。")
    if klines:
        first_ts = _normalize_candle_start(int(klines[0][0]), interval_ms)
        last_ts  = _normalize_candle_start(int(klines[-1][0]), interval_ms)
        logger.info(f"[{symbol}] {exchange_id} fetch 首/尾: {format_timestamp(first_ts)} -> {format_timestamp(last_ts)}")

    # 2) 若为空，退化为“最近 N 根”
    if not klines:
        alt = connector.fetch_ohlcv_history(symbol, timeframe, start_time_ms=None)
        alt = alt[-RECENT_LIMIT:] if alt else []
        logger.warning(f"[{symbol}] {exchange_id} 退化拉取：最近 {RECENT_LIMIT} 根，得到 {len(alt)} 条。")
        klines = alt

    if not klines:
        logger.error(f"[{symbol}] {exchange_id} 最终无可入库数据。")
        return

    _upsert_klines(conn, cursor, table, exchange_id, symbol, klines, interval_ms)

# ================== 告警去重/冷却 ==================
LAST_PRICE_ALERT_END_TS: Dict[str, int] = {}
LAST_ONE_LINE_ALERT_END_TS: Dict[str, int] = {}
LAST_ALERT_TIME: Dict[Tuple[str, str], float] = {}

def _cooldown_ok(symbol: str, kind: str, cooldown_minutes: int) -> bool:
    if cooldown_minutes <= 0: return True
    key = (symbol, kind)
    now = time.time()
    last = LAST_ALERT_TIME.get(key, 0)
    if now - last >= cooldown_minutes * 60:
        LAST_ALERT_TIME[key] = now
        return True
    return False

# ================== 价格偏差：A(BITDA) vs B(BINANCE) ==================
def _check_price_deviation(cursor, table: str, symbol: str, A_ID: str, B_ID: str,
                           thresholds: Tuple[bool, float, float, float, float],
                           config: dict, cooldown_min: int, interval_ms: int):
    enabled, th_open, th_high, th_low, th_close = thresholds
    if not enabled:
        logger.info(f"[{symbol}] 价格偏差告警关闭，跳过。")
        return

    rows_a = _select_recent_rows(cursor, table, symbol, A_ID, RECENT_LIMIT, interval_ms)
    rows_b = _select_recent_rows(cursor, table, symbol, B_ID, RECENT_LIMIT, interval_ms)
    if not rows_a or not rows_b:
        logger.warning(f"[{symbol}] 价格对比：A/B 任一侧无数据，跳过。")
        return

    set_a = {r['ts_ms'] for r in rows_a}
    set_b = {r['ts_ms'] for r in rows_b}
    commons = sorted(list(set_a & set_b))
    if not commons:
        # 打印两侧最近三根时间，帮助定位
        def head_ts(arr): return [format_timestamp(x['ts_ms']) for x in arr[:3]]
        logger.warning(f"[{symbol}] 价格对比：最近窗口 A/B 无共同时间戳。A最近3根: {head_ts(rows_a)} | B最近3根: {head_ts(rows_b)}")
        return

    kline_ts = commons[-1]
    kline_start_str = format_timestamp(kline_ts)

    if LAST_PRICE_ALERT_END_TS.get(symbol) == kline_ts:
        logger.info(f"[{symbol}] 价格对比：同一窗口已处理（K线开始: {kline_start_str}），抑制重复。")
        return

    rec_a = next(r for r in rows_a if r['ts_ms'] == kline_ts)
    rec_b = next(r for r in rows_b if r['ts_ms'] == kline_ts)

    def to_f(x):
        try: return float(x)
        except Exception: return 0.0

    ao, ah, al, ac = map(to_f, (rec_a['open'], rec_a['high'], rec_a['low'], rec_a['close']))
    bo, bh, bl, bc = map(to_f, (rec_b['open'], rec_b['high'], rec_b['low'], rec_b['close']))

    def rel(a, b):
        return 0.0 if b == 0 else (a - b) / b

    pairs = {
        "OPEN":  (ao, bo, (ao - bo), abs(ao - bo), abs(rel(ao, bo)), th_open),
        "HIGH":  (ah, bh, (ah - bh), abs(ah - bh), abs(rel(ah, bh)), th_high),
        "LOW":   (al, bl, (al - bl), abs(al - bl), abs(rel(al, bl)), th_low),
        "CLOSE": (ac, bc, (ac - bc), abs(ac - bc), abs(rel(ac, bc)), th_close),
    }

    header = f"{'项':<6}|{('A('+A_ID+')'):<16}|{('B('+B_ID+')'):<16}|{'差值(A-B)':<14}|{'绝对差':<14}|{'相对差':<10}|{'阈值':<10}|结果"
    sep = "-" * (6+1+16+1+16+1+14+1+14+1+10+1+10+1+4)
    logger.info(f"[{symbol}] 价格对比明细（比对K线开始: {kline_start_str}）：")
    logger.info(header); logger.info(sep)
    breaches = []
    for name in ("OPEN","HIGH","LOW","CLOSE"):
        a_val, b_val, diff, abs_diff, rel_diff, thr = pairs[name]
        over = rel_diff > thr
        result = "🚨 超阈" if over else "✅ 正常"
        logger.info(f"{name:<6}|{a_val:<16.6g}|{b_val:<16.6g}|{diff:<14.6g}|{abs_diff:<14.6g}|{rel_diff:<10.2%}|{thr:<10.2%}|{result}")
        if over:
            breaches.append((name, a_val, b_val, diff, abs_diff, rel_diff, thr))
    logger.info(sep)

    if not breaches:
        logger.info(f"[{symbol}] 价格对比：✅ 全部未超阈（K线开始: {kline_start_str}）。")
        LAST_PRICE_ALERT_END_TS[symbol] = kline_ts
        return

    if not _cooldown_ok(symbol, "price", cooldown_min):
        logger.info(f"[{symbol}] 价格对比：处于冷却期，跳过发送（K线开始: {kline_start_str}）。")
        LAST_PRICE_ALERT_END_TS[symbol] = kline_ts
        return

    lines = [
        f"- {n}: A={a:.6g}, B={b:.6g}, 差值(A-B)={d:.6g}, 绝对差={ad:.6g}, 相对差={rd:.2%}（阈值={t:.2%}）"
        for (n, a, b, d, ad, rd, t) in breaches
    ]
    title = f"❗ K线价格偏差告警: {symbol} | 开始 {kline_start_str}"
    text = (
        f"A({A_ID}) vs B({B_ID}) 在该根 K 线出现超阈项：\n" +
        "\n".join(lines)
    )
    # ====== 改为 Teams 通知 ======
    notify(config, title, text, dedup_key=f"KLINE_DEV|{symbol}|{kline_ts}")
    LAST_PRICE_ALERT_END_TS[symbol] = kline_ts

# ================== 一字线（仅 BITDA） ==================
def _check_one_line(cursor, table: str, symbol: str, A_ID: str,
                    params: Tuple[bool, int, float, int, int],
                    config: dict, interval_ms: int):
    enabled, count_threshold, eps, require_streak, cooldown_min = params
    if not enabled:
        logger.info(f"[{symbol}] 一字线告警关闭，跳过。")
        return

    rows = _select_recent_rows(cursor, table, symbol, A_ID, RECENT_LIMIT, interval_ms)
    if not rows:
        logger.warning(f"[{symbol}] 一字线：BITDA 无K线数据，跳过。"); return

    latest_ts = rows[0]['ts_ms']
    formatted_end = format_timestamp(latest_ts)

    one_count = 0
    for r in rows:
        try:
            o, h, l, c = float(r['open']), float(r['high']), float(r['low']), float(r['close'])
        except Exception:
            break
        if _is_one_line(o, h, l, c, eps):
            one_count += 1
        else:
            break

    logger.info(f"[{formatted_end}][{symbol}] 一字线连续：{one_count} 条（阈值 {count_threshold}，eps={eps}）")

    if one_count >= count_threshold:
        end_ts = int(latest_ts)
        if LAST_ONE_LINE_ALERT_END_TS.get(symbol) == end_ts:
            logger.info(f"[{symbol}] 一字线：同一窗口已报警，跳过重复。")
            return

        if not _cooldown_ok(symbol, "one_line", cooldown_min):
            logger.info(f"[{symbol}] 一字线：处于冷却期，跳过发送。")
            LAST_ONE_LINE_ALERT_END_TS[symbol] = end_ts
            return

        start_ts = rows[one_count - 1]['ts_ms'] if one_count - 1 < len(rows) else latest_ts
        title = f"❗ K线异常告警: {symbol} 连续一字线 ({one_count} 条)"
        text = (
            f"平台 {A_ID} 的 {symbol} 连续 {one_count} 个周期出现一字线。\n"
            f"时间范围: {format_timestamp(start_ts)} -> {formatted_end}\n"
            f"(阈值: {count_threshold} 条, epsilon={eps})"
        )
        # ====== 改为 Teams 通知 ======
        notify(config, title, text, dedup_key=f"ONE_LINE|{symbol}|{end_ts}")
        LAST_ONE_LINE_ALERT_END_TS[symbol] = end_ts

# ================== 主流程 ==================
def check_kline_alerts(conn, config):
    symbols = _get_symbols(config)
    if not symbols:
        return

    ex_conf = config['EXCHANGE_CONFIG']
    table_names = config['TABLE_NAMES']

    A_ID = ex_conf['PLATFORM_A_ID'].upper()
    B_ID = ex_conf['BENCHMARK_ID'].upper()
    A_URL = ex_conf.get('PLATFORM_A_API_URL', '').rstrip('/')
    B_URL = ex_conf.get('BINANCE_API_URL', '').rstrip('/')
    timeframe = ex_conf.get('TIME_FRAME', '1m')
    interval_ms = _interval_ms_from_timeframe(timeframe)
    cooldown_min = int((config.get('ALERT_CONFIG') or {}).get('ALERT_COOLDOWN_MINUTES', 0))

    if not A_URL or not B_URL:
        logger.critical("EXCHANGE_CONFIG 缺少 PLATFORM_A_API_URL 或 BINANCE_API_URL，请在 config.toml 中补全。")
        return

    conn_a = PlatformConnector(A_ID, A_URL)
    conn_b = PlatformConnector(B_ID, B_URL)

    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    logger.info("=" * 70)
    logger.info(f"📢 开始：拉取最新K线入库 → 一字线（{A_ID}）+ 四价偏差（{A_ID} vs {B_ID}），TF={timeframe}, interval_ms={interval_ms}")

    for symbol in symbols:
        try:
            # 1) 拉取并入库（两侧）
            _ingest_latest_for(conn, cursor, table_names['KLINE_DATA'], conn_a, A_ID, symbol, timeframe, interval_ms)
            _ingest_latest_for(conn, cursor, table_names['KLINE_DATA'], conn_b, B_ID, symbol, timeframe, interval_ms)

            # 2) 价格偏差（最新共同K线）
            price_thresholds = _read_price_thresholds(config, symbol)
            _check_price_deviation(cursor, table_names['KLINE_DATA'], symbol, A_ID, B_ID,
                                   price_thresholds, config, cooldown_min, interval_ms)

            # 3) 一字线（A_ID）
            one_line_params = _read_one_line_thresholds(config, symbol)
            _check_one_line(cursor, table_names['KLINE_DATA'], symbol, A_ID, one_line_params, config, interval_ms)

        except pymysql.Error as db_err:
            logger.error(f"[{symbol}] 数据库失败: {db_err}", exc_info=True)
            conn.rollback()
        except Exception as e:
            logger.critical(f"[{symbol}] 监控发生未知异常: {e}", exc_info=True)

    cursor.close()
    logger.info("🎉 本次监控周期完成。")
    logger.info("=" * 70)

def main():
    setup_logging()
    conn = None
    try:
        config = load_config()
        conn = init_db(config)
        frequency = int(config['EXCHANGE_CONFIG'].get('FREQUENCY_SECONDS', 60))
        logger.info(f"K 线监控脚本已启动，运行频率为每 {frequency} 秒一次...")

        while True:
            config = load_config()
            check_kline_alerts(conn, config)
            time.sleep(frequency)

    except KeyboardInterrupt:
        logger.info("用户中断程序 (Ctrl+C)。程序正在退出...")
    except Exception as e:
        logger.critical(f"K 线脚本发生致命错误，正在退出: {e}", exc_info=True)
    finally:
        if conn:
            conn.close(); logger.info("数据库连接已关闭。程序退出。")

if __name__ == '__main__':
    main()
