# daily_report_kline_volume.py
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Union

import pymysql
from utils import load_config, init_db

# --- 通知模块：Teams（必有，不存在就不报错） ---
try:
    from teams_alerter import send_teams_alert
except ImportError:
    def send_teams_alert(*args, **kwargs):
        logging.getLogger('monitor_system').warning("teams_alerter 未找到，跳过 Teams 发送。")

# --- 保留：Lark（可选） ---
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
    fmt = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'daily_report_log.log')
    fh = logging.FileHandler(log_file, encoding='utf-8'); fh.setLevel(logging.INFO); fh.setFormatter(fmt)
    root = logging.getLogger('monitor_system'); root.addHandler(ch); root.addHandler(fh); root.setLevel(logging.INFO)

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
        return datetime.utcfromtimestamp(_to_float(ts)/1000.0).strftime('%Y-%m-%d %H:%M')
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
    if ts_mode == TsMode.MS_INT:
        start_ms = int(start_utc.timestamp()*1000)
        end_ms   = int(end_utc.timestamp()*1000)
        return "timestamp >= %s AND timestamp < %s", (start_ms, end_ms)
    else:
        return "timestamp >= %s AND timestamp < %s", (start_utc, end_utc)

# ---------- 配置读取 ----------
def read_price_thresholds(config: dict, symbol: str) -> Dict[str, float]:
    """读取四价偏差阈值（相对B）：支持全局 + 按标的覆盖"""
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}
    def pick(name: str, default: float) -> float:
        sv, _ = _ci_get(sym_conf, name); gv, _ = _ci_get(ac, name)
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
    """读取成交量目标系数与容差：支持全局 + 按标的覆盖"""
    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = _ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}
    tr_sym, _ = _ci_get(sym_conf, 'VOLUME_TARGET_RATIO')
    tr_glb, _ = _ci_get(ac,       'VOLUME_TARGET_RATIO')
    target_ratio = tr_sym if tr_sym is not None else (tr_glb if tr_glb is not None else 0.20)
    tol_sym, _ = _ci_get(sym_conf, 'VOLUME_RATIO_THRESHOLD')
    tol_glb, _ = _ci_get(ac,       'VOLUME_RATIO_THRESHOLD')
    tolerance = tol_sym if tol_sym is not None else (tol_glb if tol_glb is not None else 0.20)
    try:
        target_ratio = float(target_ratio)
    except Exception:
        target_ratio = 0.20
    try:
        tolerance = float(tolerance)
    except Exception:
        tolerance = 0.20
    return target_ratio, tolerance

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
    """
    price_stats: {'counts':{O,H,L,C}, 'exceeds':[{ts,field,a,b,abs,rel}, ...]}
    volume_stats: {'A_sum','B_sum','target','diff_abs','diff_rel'}
    """
    map_a = {r['timestamp']: r for r in rows_a}
    map_b = {r['timestamp']: r for r in rows_b}
    commons = sorted(set(map_a.keys()) & set(map_b.keys()))

    counts = {'OPEN':0, 'HIGH':0, 'LOW':0, 'CLOSE':0}
    exceeds: List[dict] = []

    def check(field_key: str, a_val: Number, b_val: Number, thr: float, ts: TS):
        if b_val is None or _to_float(b_val) == 0.0:
            return
        rel = abs(_to_float(a_val) - _to_float(b_val)) / abs(_to_float(b_val))
        if rel > thr:
            counts[field_key] += 1
            exceeds.append({
                'ts': ts, 'field': field_key,
                'a': _to_float(a_val), 'b': _to_float(b_val),
                'abs': _to_float(a_val) - _to_float(b_val),
                'rel': rel
            })

    for ts in commons:
        ra = map_a[ts]; rb = map_b[ts]
        check('OPEN',  ra['open'],  rb['open'],  price_thresholds['OPEN'],  ts)
        check('HIGH',  ra['high'],  rb['high'],  price_thresholds['HIGH'],  ts)
        check('LOW',   ra['low'],   rb['low'],   price_thresholds['LOW'],   ts)
        check('CLOSE', ra['close'], rb['close'], price_thresholds['CLOSE'], ts)

    A_sum = sum(_to_float(map_a[ts]['volume']) for ts in commons)
    B_sum = sum(_to_float(map_b[ts]['volume']) for ts in commons)
    target = volume_target_ratio * B_sum
    diff_abs = A_sum - target
    diff_rel = (diff_abs / target) if target != 0 else (0.0 if diff_abs == 0 else float('inf'))

    price_stats = {'counts': counts, 'exceeds': exceeds}
    volume_stats = {'A_sum': A_sum, 'B_sum': B_sum, 'target': target, 'diff_abs': diff_abs, 'diff_rel': diff_rel}
    return price_stats, volume_stats

# ---------- Markdown 报告 ----------
def render_markdown(report_date_str: str, timeframe: str,
                    start_utc: datetime, end_utc: datetime,
                    per_symbol: Dict[str, dict]) -> str:
    lines: List[str] = []
    lines.append(f"# 监控日报（UTC） - {report_date_str}")
    lines.append("")
    lines.append(f"- 统计区间：{start_utc.strftime('%Y-%m-%d %H:%M')} ~ {end_utc.strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append(f"- 时间粒度：{timeframe}")
    lines.append("")

    total_price_exceeds = 0
    for _, d in per_symbol.items():
        c = d['price']['counts']
        total_price_exceeds += (c['OPEN'] + c['HIGH'] + c['LOW'] + c['CLOSE'])
    lines.append(f"**全量汇总：四价越阈总次数 = {total_price_exceeds}**")
    lines.append("")

    for sym, d in per_symbol.items():
        price = d['price']; vol = d['volume']; c = price['counts']
        lines.append(f"## {sym}")
        lines.append("")
        lines.append(f"**四价越阈次数**：OPEN={c['OPEN']} | HIGH={c['HIGH']} | LOW={c['LOW']} | CLOSE={c['CLOSE']}")
        lines.append("")
        if price['exceeds']:
            lines.append("<details><summary>明细（全部越阈点）</summary>")
            lines.append("")
            lines.append("| 时间(UTC) | 字段 | A值 | B值 | 绝对偏差 | 相对偏差 |")
            lines.append("|---|---|---:|---:|---:|---:|")
            for item in price['exceeds']:
                lines.append(f"| {_fmt_ts(item['ts'])} | {item['field']} | "
                             f"{item['a']:.6g} | {item['b']:.6g} | {item['abs']:.6g} | {_fmt_pct(item['rel'])} |")
            lines.append("")
            lines.append("</details>")
        else:
            lines.append("_四价均未越阈。_")
        lines.append("")
        lines.append("**成交量（按天累计）**")
        lines.append("")
        lines.append(f"- A 累计成交量：{vol['A_sum']:.6f}")
        lines.append(f"- B 累计成交量：{vol['B_sum']:.6f}")
        lines.append(f"- 目标（r×B）：{vol['target']:.6f}")
        lines.append(f"- 偏差（绝对）：{vol['diff_abs']:.6f}")
        lines.append(f"- 偏差（相对）：{_fmt_pct(vol['diff_rel'])}")
        lines.append("")
    return "\n".join(lines)

# ---------- 飞书/Teams 摘要（全量标的，自动分条） ----------
def render_summary_chunks(report_date_str: str,
                          start_utc: datetime, end_utc: datetime,
                          per_symbol: Dict[str, dict],
                          max_chars: int = 2500) -> List[Tuple[str, str]]:
    """
    生成【多条】摘要（覆盖所有标的，不截断）。
    每条消息 <= max_chars（粗略控制），依次发送。
    返回 [(title, text), ...]
    """
    total_price_ex = 0
    vol_exceed_count = 0
    exceed_lines: List[str] = []
    normal_lines: List[str] = []

    for sym, d in per_symbol.items():
        c = d['price']['counts']
        total_ex_sym = c['OPEN'] + c['HIGH'] + c['LOW'] + c['CLOSE']
        total_price_ex += total_ex_sym

        vol = d['volume']
        r   = d['volume_ratio']
        tol = d['volume_tolerance']
        dev = vol['diff_rel']
        dev_str = _fmt_pct(dev)

        line = f"[{total_ex_sym}] {sym}: O={c['OPEN']} H={c['HIGH']} L={c['LOW']} C={c['CLOSE']} | Vol dev={dev_str} (r={r:.2f})"
        if abs(dev) > tol:
            line += " 【EXCEED】"
            exceed_lines.append(line)
            vol_exceed_count += 1
        else:
            normal_lines.append(line)

    lines_all: List[str] = []
    lines_all.extend(exceed_lines)
    lines_all.extend(normal_lines)

    header_full = (
        f"时间：{start_utc.strftime('%Y-%m-%d %H:%M')} ~ {end_utc.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"标的数：{len(per_symbol)}\n"
        f"四价越阈总次数：{total_price_ex}\n"
        f"成交量超阈标的数：{vol_exceed_count}\n\n"
    )
    header_cont = "（续）以下为其余标的统计：\n\n"

    chunks: List[Tuple[str, str]] = []
    acc_lines: List[str] = []
    acc_len = 0

    def flush_chunk(idx: int, total: int, is_first: bool):
        nonlocal acc_lines
        if not acc_lines:
            return
        title = f"📊 日报（K线+成交量统计）UTC {report_date_str}（{idx}/{total}）"
        head = header_full if is_first else header_cont
        body = head + "\n".join(acc_lines)
        chunks.append((title, body))
        acc_lines = []

    for line in lines_all:
        delta = len(line) + 1
        reserve = 600 if not chunks else 200
        if (acc_len + delta + reserve) > max_chars and acc_lines:
            flush_chunk(idx=len(chunks)+1, total=9999, is_first=(len(chunks)==0))
            acc_len = 0
        acc_lines.append(line)
        acc_len += delta

    if acc_lines:
        flush_chunk(idx=len(chunks)+1, total=9999, is_first=(len(chunks)==0))

    fixed: List[Tuple[str, str]] = []
    total_n = len(chunks)
    for i, (title, body) in enumerate(chunks, 1):
        new_title = title.rsplit('（', 1)[0] + f"（{i}/{total_n}）"
        fixed.append((new_title, body))
    return fixed

# ---------- 主流程 ----------
def main():
    setup_logging()
    conn = None
    try:
        config = load_config()
        conn = init_db(config)
        try:
            conn.autocommit(True)
            with conn.cursor() as c:
                c.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass

        teams_cfg = config.get('TEAMS_NOTIFY') or {}
        lark_cfg  = config.get('LARK_APP_CONFIG') or {}

        table = (config.get('TABLE_NAMES') or {}).get('KLINE_DATA') or 'kline_data'
        ex = config.get('EXCHANGE_CONFIG') or {}
        A_ID = (ex.get('PLATFORM_A_ID') or 'BITDA_FUTURES').upper()
        B_ID = (ex.get('BENCHMARK_ID') or 'BINANCE_FUTURES').upper()
        timeframe = (ex.get('TIME_FRAME') or '1m').lower()
        symbols = (config.get('MONITORED_SYMBOLS') or (ex.get('MONITORED_SYMBOLS') or []))
        if not symbols:
            logger.critical("配置缺少 MONITORED_SYMBOLS。")
            return

        # UTC 昨天区间
        today_utc = datetime.now(timezone.utc).date()
        start_utc = datetime.combine(today_utc - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        end_utc   = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
        report_date_str = (today_utc - timedelta(days=1)).strftime("%Y-%m-%d")

        ts_mode = detect_ts_mode(conn, table)
        logger.info(f"[日报] 时间范围（UTC）：{start_utc} ~ {end_utc} | ts_mode={ts_mode}")

        per_symbol: Dict[str, dict] = {}

        for sym in symbols:
            # 读取阈值 & 成交量参数
            price_thr = read_price_thresholds(config, sym)
            vol_ratio, vol_tol = read_volume_params(config, sym)

            # 阈值确认日志
            logger.info(
                f"[{sym}] 阈值确认 | OPEN={price_thr['OPEN']:.4%}, HIGH={price_thr['HIGH']:.4%}, "
                f"LOW={price_thr['LOW']:.4%}, CLOSE={price_thr['CLOSE']:.4%} | "
                f"VOLUME: r={vol_ratio:.2f}, tol={vol_tol:.2%}"
            )

            rows_a = fetch_ohlc_in_range(conn, table, sym, A_ID, ts_mode, start_utc, end_utc)
            rows_b = fetch_ohlc_in_range(conn, table, sym, B_ID, ts_mode, start_utc, end_utc)

            price_stats, volume_stats = aggregate_daily_for_symbol(rows_a, rows_b, price_thr, vol_ratio)
            per_symbol[sym] = {
                'price': price_stats,
                'volume': volume_stats,
                'volume_ratio': vol_ratio,
                'volume_tolerance': vol_tol,
            }

        # 写 Markdown 明细到子目录 daily_report_kline_volume/
        base_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir  = os.path.join(base_dir, "daily_report_kline_volume")
        os.makedirs(out_dir, exist_ok=True)
        md = render_markdown(report_date_str, timeframe, start_utc, end_utc, per_symbol)
        out_name = f"daily_report_{report_date_str}_UTC.md"
        out_path = os.path.join(out_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        logger.info(f"[日报] 已生成：{out_path}")

        # 摘要分条 → 发送到 Teams（也可保留 Lark 双发）
        chunks = render_summary_chunks(report_date_str, start_utc, end_utc, per_symbol)
        for title, text in chunks:
            try:
                send_teams_alert(teams_cfg, title, text, severity="info")
            except Exception as e:
                logger.warning(f"Teams 摘要发送失败：{e} | 标题={title}")
            try:
                send_lark_alert(lark_cfg, title, text)
            except Exception as e:
                logger.warning(f"Lark 摘要发送失败：{e} | 标题={title}")

    except Exception as e:
        logger.critical(f"日报生成失败：{e}", exc_info=True)
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
            logger.info("数据库连接已关闭。")

if __name__ == "__main__":
    main()
