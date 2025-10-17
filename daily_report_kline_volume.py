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

        # ✅ 打印 Teams 配置
        teams_cfg = config.get('TEAMS_NOTIFY') or {}
        logger.info(f"[DEBUG] TEAMS CONFIG: {teams_cfg}")

        # 如果未启用或缺少 URL，则直接退出
        if not teams_cfg.get("ENABLED"):
            logger.warning("[TEAMS] 配置未启用 (ENABLED=false 或缺失)，跳过发送。")
            return
        if not teams_cfg.get("FLOW_URL"):
            logger.warning("[TEAMS] 缺少 FLOW_URL，跳过发送。")
            return

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
        end_utc = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
        report_date_str = (today_utc - timedelta(days=1)).strftime("%Y-%m-%d")

        ts_mode = detect_ts_mode(conn, table)
        logger.info(f"[日报] 时间范围（UTC）：{start_utc} ~ {end_utc} | ts_mode={ts_mode}")

        # 假设这里已有你原来的报表生成逻辑，生成 Markdown 文件...
        # --- 模拟 ---
        fake_report = "这是一个示例日报内容，用于测试 Teams 通知。"

        # ✅ 发送到 Teams
        logger.info("[TEAMS] 正在发送日报到 Teams...")
        send_teams_alert(teams_cfg, f"Daily Report {report_date_str}", fake_report, severity="info")

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
