# daily_report_kline_volume.py
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Union
import pymysql
from utils import load_config, init_db

# --- 通知模块：Teams（必有） ---
try:
    from teams_alerter import send_teams_alert
except ImportError:
    def send_teams_alert(*args, **kwargs):
        logging.getLogger('monitor_system').warning("teams_alerter 未找到，跳过 Teams 发送。")

# --- 可选：Lark ---
try:
    from lark_alerter import send_lark_alert
except ImportError:
    def send_lark_alert(*args, **kwargs):
        logging.getLogger('monitor_system').warning("lark_alerter 未找到，跳过 Lark 发送。")

logger = logging.getLogger('monitor_system')

# ---------- 日志 ----------
def setup_logging():
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'daily_report_log.log')
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root = logging.getLogger('monitor_system')
    root.addHandler(ch)
    root.addHandler(fh)
    root.setLevel(logging.INFO)

# ---------- 工具 ----------
Number = Union[int, float]
TS = Union[int, float, datetime]

def _ci_get(d: dict, key: str):
    if not isinstance(d, dict):
        return None, None
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

def _fmt_pct(x: float) -> str:
    try:
        return f"{x:+.2%}"
    except Exception:
        return "N/A"

def _fmt_ts(ts: TS) -> str:
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M')
    try:
        return datetime.utcfromtimestamp(_to_float(ts) / 1000.0).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return "N/A"

# ---------- 时间戳模式 ----------
class TsMode:
    MS_INT = "ms_int"
    DATETIME = "datetime"

def detect_ts_mode(conn, table: str) -> str:
    sql = f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1"
    with conn.cursor() as c:
        c.execute(sql)
        row = c.fetchone()
    if not row:
        return TsMode.MS_INT
    ts = row[0] if isinstance(row, (list, tuple)) else row.get('timestamp')
    return TsMode.DATETIME if isinstance(ts, datetime) else TsMode.MS_INT

def make_range_predicate(ts_mode: str, start_utc: datetime, end_utc: datetime):
    if ts_mode == TsMode.DATETIME:
        return "timestamp >= %s AND timestamp < %s", (start_utc, end_utc)
    else:
        return "timestamp >= %s AND timestamp < %s", (
            int(start_utc.timestamp() * 1000), int(end_utc.timestamp() * 1000))

# ---------- 阈值读取 ----------
def read_price_thresholds(config: dict, symbol: str) -> Dict[str, float]:
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}

    def pick(name: str, default: float) -> float:
        sv, _ = _ci_get(sym_conf, name)
        gv, _ = _ci_get(ac, name)
        val = sv if sv is not None else (gv if gv is not None else default)
        try:
            return float(val)
        except Exception:
            return default

    return {
        "OPEN":  pick('OPEN_DEVIATION_THRESHOLD',  0.002),
        "HIGH":  pick('HIGH_DEVIATION_THRESHOLD',  0.001),
        "LOW":   pick('LOW_DEVIATION_THRESHOLD',   0.001),
        "CLOSE": pick('CLOSE_DEVIATION_THRESHOLD', 0.0005),
    }

def read_volume_params(config: dict, symbol: str) -> Tuple[float, float]:
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}
    tr_sym, _ = _ci_get(sym_conf, 'VOLUME_TARGET_RATIO')
    tr_glb, _ = _ci_get(ac, 'VOLUME_TARGET_RATIO')
    target_ratio = tr_sym if tr_sym is not None else (tr_glb if tr_glb is not None else 0.20)
    tol_sym, _ = _ci_get(sym_conf, 'VOLUME_RATIO_THRESHOLD')
    tol_glb, _ = _ci_get(ac, 'VOLUME_RATIO_THRESHOLD')
    tolerance = tol_sym if tol_sym is not None else (tol_glb if tol_glb is not None else 0.20)
    try:
        return float(target_ratio), float(tolerance)
    except Exception:
        return 0.2, 0.2

# ---------- DB 拉取 ----------
def fetch_ohlc_in_range(conn, table: str, symbol: str, exchange: str,
                        ts_mode: str, start_utc: datetime, end_utc: datetime) -> List[dict]:
    where, params = make_range_predicate(ts_mode, start_utc, end_utc)
    sql = f"""
        SELECT timestamp, `open`, `high`, `low`, `close`, volume
        FROM {table}
        WHERE symbol=%s AND exchange=%s AND {where}
        ORDER BY timestamp ASC
    """
    args = (symbol, exchange, *params)
    with conn.cursor(cursor=pymysql.cursors.DictCursor) as c:
        c.execute(sql, args)
        return c.fetchall() or []

# ---------- 统计 ----------
def aggregate_daily_for_symbol(rows_a: List[dict], rows_b: List[dict],
                               price_thresholds: Dict[str, float],
                               volume_target_ratio: float):
    map_a = {r['timestamp']: r for r in rows_a}
    map_b = {r['timestamp']: r for r in rows_b}
    commons = sorted(set(map_a.keys()) & set(map_b.keys()))

    counts = {'OPEN': 0, 'HIGH': 0, 'LOW': 0, 'CLOSE': 0}
    exceeds: List[dict] = []

    def check(field_key: str, a_val, b_val, thr, ts):
        if b_val is None or _to_float(b_val) == 0.0:
            return
        rel = abs(_to_float(a_val) - _to_float(b_val)) / abs(_to_float(b_val))
        if rel > thr:
            counts[field_key] += 1
            exceeds.append({'ts': ts, 'field': field_key, 'a': a_val, 'b': b_val, 'rel': rel})

    for ts in commons:
        ra, rb = map_a[ts], map_b[ts]
        for field in ['open', 'high', 'low', 'close']:
            check(field.upper(), ra[field], rb[field], price_thresholds[field.upper()], ts)

    sum_a = sum(_to_float(map_a[t]['volume']) for t in commons)
    sum_b = sum(_to_float(map_b[t]['volume']) for t in commons)
    target = sum_b * volume_target_ratio
    diff_abs = sum_a - target
    diff_rel = diff_abs / target if target else 0.0
    volume_stats = {
        'A_sum': sum_a, 'B_sum': sum_b,
        'target': target, 'diff_abs': diff_abs, 'diff_rel': diff_rel
    }
    return {'counts': counts, 'exceeds': exceeds}, volume_stats

# ---------- Markdown 渲染 ----------
def render_summary_chunks(report_date_str, start_utc, end_utc, per_symbol: Dict[str, dict]) -> List[Tuple[str, str]]:
    total_price_ex = sum(sum(v['price']['counts'].values()) for v in per_symbol.values())
    vol_exceed_count = sum(1 for v in per_symbol.values() if abs(v['volume']['diff_rel']) > v['volume_tolerance'])

    header = (
        f"📊 日报（K线+成交量统计）UTC {report_date_str}\n\n"
        f"时间：{start_utc.strftime('%Y-%m-%d %H:%M')} ~ {end_utc.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"标的数：{len(per_symbol)}\n"
        f"四价越阈总次数：{total_price_ex}\n"
        f"成交量超阈标的数：{vol_exceed_count}\n\n"
    )

    # 每个标的单独换行，使用 Markdown 两个空格 + \n 强制换行
    body_lines = []
    for sym, v in per_symbol.items():
        c = v['price']['counts']
        dev = v['volume']['diff_rel']
        r = v['volume_ratio']
        body_lines.append(
            f"[{sum(c.values())}] {sym}: O={c['OPEN']} H={c['HIGH']} L={c['LOW']} C={c['CLOSE']} | "
            f"Vol dev={_fmt_pct(dev)} (r={r:.2f})  \n"  # ← 两个空格 + \n 表示换行
        )

    body = header + "".join(body_lines)
    return [(f"📊 日报（K线+成交量统计）UTC {report_date_str}", body)]


# ---------- 主流程 ----------
def main():
    setup_logging()
    conn = None
    try:
        config = load_config()
        conn = init_db(config)
        teams_cfg = config.get('TEAMS_NOTIFY') or {}
        lark_cfg = config.get('LARK_APP_CONFIG') or {}

        table = (config.get('TABLE_NAMES') or {}).get('KLINE_DATA') or 'kline_data'
        ex = config.get('EXCHANGE_CONFIG') or {}
        A_ID = (ex.get('PLATFORM_A_ID') or 'BITDA_FUTURES').upper()
        B_ID = (ex.get('BENCHMARK_ID') or 'BINANCE_FUTURES').upper()
        timeframe = (ex.get('TIME_FRAME') or '1m').lower()
        symbols = (config.get('MONITORED_SYMBOLS') or (ex.get('MONITORED_SYMBOLS') or []))
        if not symbols:
            logger.critical("配置缺少 MONITORED_SYMBOLS。")
            return

        today_utc = datetime.now(timezone.utc).date()
        start_utc = datetime.combine(today_utc - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        end_utc = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
        report_date_str = (today_utc - timedelta(days=1)).strftime("%Y-%m-%d")

        ts_mode = detect_ts_mode(conn, table)
        logger.info(f"[日报] 时间范围（UTC）：{start_utc} ~ {end_utc} | ts_mode={ts_mode}")

        per_symbol: Dict[str, dict] = {}
        for sym in symbols:
            price_thr = read_price_thresholds(config, sym)
            vol_ratio, vol_tol = read_volume_params(config, sym)
            rows_a = fetch_ohlc_in_range(conn, table, sym, A_ID, ts_mode, start_utc, end_utc)
            rows_b = fetch_ohlc_in_range(conn, table, sym, B_ID, ts_mode, start_utc, end_utc)
            price_stats, volume_stats = aggregate_daily_for_symbol(rows_a, rows_b, price_thr, vol_ratio)
            per_symbol[sym] = {'price': price_stats, 'volume': volume_stats,
                               'volume_ratio': vol_ratio, 'volume_tolerance': vol_tol}

        base_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.join(base_dir, "daily_report_kline_volume")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"daily_report_{report_date_str}_UTC.md")
        with open(out_path, "w", encoding="utf-8") as f:
            for _, text in render_summary_chunks(report_date_str, start_utc, end_utc, per_symbol):
                f.write(text)
        logger.info(f"[日报] 已生成：{out_path}")

        # 发送到 Teams
        for title, text in render_summary_chunks(report_date_str, start_utc, end_utc, per_symbol):
            send_teams_alert(teams_cfg, title, text, severity="info")

    except Exception as e:
        logger.critical(f"日报生成失败：{e}", exc_info=True)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            logger.info("数据库连接已关闭。")

if __name__ == "__main__":
    main()
