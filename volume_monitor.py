import time
import logging
from datetime import datetime
import pymysql
import sys
import os
from typing import Union, Tuple
import utils as _u

# ===== 导入工具与告警模块 =====
try:
    from utils import load_config, init_db
except ImportError:
    print("FATAL ERROR: 缺少 utils.py 文件，无法导入配置和DB连接函数。")
    sys.exit(1)

try:
    from lark_alerter import send_lark_alert
except ImportError:
    def send_lark_alert(*args, **kwargs):
        logging.getLogger('monitor_system').error("Lark告警模块未找到，无法发送通知。")

# 全局 logger
logger = logging.getLogger('monitor_system')


# ===== 日志配置 =====
def setup_logging():
    """控制台 + 文件(./volume_monitor_log.log)"""
    logger_name = 'monitor_system'
    root_logger = logging.getLogger(logger_name)

    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)

        fmt = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)

        log_dir = os.path.dirname(os.path.abspath(__file__))
        log_file_path = os.path.join(log_dir, 'volume_monitor_log.log')
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)

        root_logger.addHandler(ch)
        root_logger.addHandler(fh)


# ===== 工具函数 =====
def format_timestamp(ts: Union[int, float, datetime, None]) -> str:
    """毫秒时间戳或 datetime -> 'YYYY-MM-DD HH:MM'（本地时间显示）"""
    if ts is None:
        return "N/A"
    if isinstance(ts, datetime):
        dt_object = ts
    elif isinstance(ts, (int, float)):
        dt_object = datetime.fromtimestamp(ts / 1000.0)
    else:
        logger.error(f"format_timestamp 收到不支持的类型: {type(ts)}")
        return "类型错误"
    return dt_object.strftime('%Y-%m-%d %H:%M')


def _read_target_and_tolerance(config: dict, symbol: str):
    """
    读取目标比例与容差（大小写/空格不敏感）：
      - target_ratio：SYMBOL覆盖 -> 全局 'VOLUME_TARGET_RATIO' -> 默认 0.20
      - tolerance  ：SYMBOL覆盖 -> 全局 'VOLUME_RATIO_THRESHOLD' -> 默认 0.20
    返回: (target_ratio, tolerance, global_hit_key, symbol_hit_key)
    """
    def ci_get(d: dict, key: str):
        """大小写不敏感、忽略首尾空格的取值，返回 (value, 命中的真实键名)"""
        if not isinstance(d, dict):
            return None, None
        target = key.strip().lower()
        for k, v in d.items():
            if isinstance(k, str) and k.strip().lower() == target:
                return v, k
        return None, None

    ac = (config or {}).get('ALERT_CONFIG', {}) or {}
    sym_map = ci_get(ac, 'SYMBOL_THRESHOLDS')[0] or {}
    sym_conf = sym_map.get(symbol) or {}

    # 目标比例
    sym_tr_val, sym_tr_key = ci_get(sym_conf, 'VOLUME_TARGET_RATIO')
    glb_tr_val, glb_tr_key = ci_get(ac,       'VOLUME_TARGET_RATIO')
    target_ratio = sym_tr_val if sym_tr_val is not None else (glb_tr_val if glb_tr_val is not None else 0.20)

    # 容差（从 VOLUME_RATIO_THRESHOLD 读取）
    sym_tol_val, sym_tol_key = ci_get(sym_conf, 'VOLUME_RATIO_THRESHOLD')
    glb_tol_val, glb_tol_key = ci_get(ac,       'VOLUME_RATIO_THRESHOLD')
    tolerance = sym_tol_val if sym_tol_val is not None else (glb_tol_val if glb_tol_val is not None else 0.20)

    # 转 float（防字符串/Decimal）
    try:
        target_ratio = float(target_ratio)
    except Exception:
        target_ratio = 0.20
    try:
        tolerance = float(tolerance)
    except Exception:
        tolerance = 0.20

    # 打印命中的真实键名，便于核对是否有空格/大小写问题
    logger.info(
        f"[{symbol}] 阈值来源命中："
        f"target_ratio={target_ratio} (symbol:{sym_tr_key or '-'} / global:{glb_tr_key or '-'}) | "
        f"tolerance={tolerance} (symbol:{sym_tol_key or '-'} / global:{glb_tol_key or '-'})"
    )
    # 为了后续旧日志兼容，返回 glb/sym 原始值（浮点）
    glb_tol_raw = float(glb_tol_val) if glb_tol_val is not None else None
    sym_tol_raw = float(sym_tol_val) if sym_tol_val is not None else None
    return target_ratio, tolerance, glb_tol_raw, sym_tol_raw



# ===== 主逻辑 =====
def compare_volume_alert(conn, config):
    """
    读取数据库最近 15 条 K 线，按“目标=target_ratio×Binance，容差=±tolerance（来自VOLUME_RATIO_THRESHOLD）”进行校验。
    """
    symbols = config['MONITORED_SYMBOLS']
    ex_conf = config['EXCHANGE_CONFIG']
    table_names = config['TABLE_NAMES']
    lark_app_config = config['LARK_APP_CONFIG']

    A_ID = ex_conf['PLATFORM_A_ID'].upper()
    B_ID = ex_conf['BENCHMARK_ID'].upper()

    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    logger.info("=" * 70)
    logger.info("📢 开始执行 15min 累计交易量对比周期...")

    for symbol in symbols:
        formatted_time_start = "N/A"
        formatted_time_end = "N/A"
        timestamps_to_compare = set()

        try:
            # --- 1) 查询 A/B 最近 15 条 ---
            sql_tpl = f"""
                SELECT timestamp, volume FROM {table_names['KLINE_DATA']}
                WHERE symbol = %s AND exchange = %s
                ORDER BY timestamp DESC
                LIMIT 15
            """
            cursor.execute(sql_tpl, (symbol, A_ID))
            rows_a = cursor.fetchall()

            cursor.execute(sql_tpl, (symbol, B_ID))
            rows_b = cursor.fetchall()

            # --- 2) 对齐并累计（将 Decimal 转 float）---
            A_15min_volume, B_15min_volume = 0.0, 0.0

            if len(rows_a) < 15 or len(rows_b) < 15:
                latest_ts_value = rows_a[0]['timestamp'] if rows_a else (rows_b[0]['timestamp'] if rows_b else None)
                formatted_time_end = format_timestamp(latest_ts_value)
                logger.warning(
                    f"[{formatted_time_end}][{symbol}] ⚠️ 数据量不足：{A_ID}({len(rows_a)}) 或 {B_ID}({len(rows_b)}) < 15，跳过对比。"
                )
            else:
                vol_map_a = {r['timestamp']: float(r['volume']) for r in rows_a}
                vol_map_b = {r['timestamp']: float(r['volume']) for r in rows_b}

                timestamps_to_compare = set(vol_map_a.keys()) & set(vol_map_b.keys())
                if len(timestamps_to_compare) < 10:
                    latest_ts_value = rows_a[0]['timestamp']
                    formatted_time_end = format_timestamp(latest_ts_value)
                    logger.warning(
                        f"[{formatted_time_end}][{symbol}] ⚠️ 对齐失败：共同时间戳不足 10 条 ({len(timestamps_to_compare)})，跳过对比。"
                    )
                else:
                    min_ts = min(timestamps_to_compare)
                    max_ts = max(timestamps_to_compare)
                    formatted_time_end = format_timestamp(max_ts)
                    formatted_time_start = format_timestamp(min_ts)

                    A_15min_volume = sum(vol_map_a[ts] for ts in timestamps_to_compare)
                    B_15min_volume = sum(vol_map_b[ts] for ts in timestamps_to_compare)

                    logger.info(f"[{formatted_time_end}][{symbol}] ✅ 数据对齐成功。共同 K 线数量: {len(timestamps_to_compare)}。")

            # --- 3) 目标与容差判断 ---
            A_volume = float(A_15min_volume)
            B_volume = float(B_15min_volume)

            target_ratio, tolerance, global_tol_raw, symbol_tol_raw = _read_target_and_tolerance(config, symbol)
            time_range_str = f"{formatted_time_start} -> {formatted_time_end}"

            # 关键调试日志：打印容差来源与最终值
            logger.info(
                f"[{symbol}] 阈值调试：target_ratio={target_ratio} | "
                f"global.VOLUME_RATIO_THRESHOLD={global_tol_raw} | "
                f"symbol.VOLUME_RATIO_THRESHOLD={symbol_tol_raw} | "
                f"采用 tolerance={tolerance}"
            )

            logger.info("-" * 70)
            logger.info(f"--- 📊 {time_range_str} | {symbol} 交易量目标校验 ({A_ID} vs {B_ID}) ---")
            logger.info(f"规则：A ≈ {target_ratio:.2f} × B，允许偏差 ±{tolerance:.0%}（相对目标值）")
            logger.info(f"交易量对比详情 (基于 {len(timestamps_to_compare)} 条共同 K线累计):")

            vol_log_lines = []
            vol_log_lines.append(
                f"{'项':<8} | {A_ID:<16} | {B_ID:<16} | {'目标值(r×B)':<14} | {'相对偏差':<10} | {'容差':<8} | 结果"
            )
            vol_log_lines.append("-" * 110)

            result_status = "---"
            rel_dev_str = "N/A"
            target_val = target_ratio * B_volume

            if B_volume == 0:
                if A_volume == 0:
                    result_status = "✅ 正常（B=0,A=0）"
                    rel_dev_str = "0.00%"
                else:
                    result_status = "🚨 异常（B=0,A>0）"
                    rel_dev_str = "Inf"
                    title = f"🚨 交易量异常: {symbol} @ {formatted_time_end} (15min 累计)"
                    text = (
                        f"**时间范围**: {time_range_str}\n"
                        f"B({B_ID})=0，但 A({A_ID})={A_volume:.2f}，无法按比例校验（目标=0）。"
                    )
                    send_lark_alert(lark_app_config, title, text)
            else:
                if target_val == 0:
                    rel_dev = 0.0 if A_volume == 0 else float('inf')
                else:
                    rel_dev = (A_volume - target_val) / target_val  # 正=高于目标；负=低于目标

                rel_dev_abs = abs(rel_dev)
                rel_dev_str = f"{rel_dev:+.2%}"
                within = rel_dev_abs <= tolerance

                if within:
                    result_status = "✅ 正常"
                else:
                    result_status = "🚨 告警"
                    title = f"🚨 成交量偏离阈值: {symbol} @ {formatted_time_end} (15min 累计)"
                    text = (
                        f"**时间范围**: {time_range_str}\n"
                        f"A({A_ID})={A_volume:.2f}\n"
                        f"B({B_ID})={B_volume:.2f}\n"
                        f"目标值 = {target_ratio:.2f} × B = {target_val:.2f}\n"
                        f"相对偏差 = (A-目标)/目标 = {rel_dev:+.2%}\n"
                        f"容差 = ±{tolerance:.0%}"
                    )
                    send_lark_alert(lark_app_config, title, text)

            vol_log_lines.append(
                f"{'Volume':<8} | {A_volume:<16.2f} | {B_volume:<16.2f} | {target_val:<14.2f} | {rel_dev_str:<10} | {tolerance:<8.0%} | {result_status}"
            )
            for line in vol_log_lines:
                logger.info(line)
            logger.info("-" * 70)

        except pymysql.Error as db_err:
            logger.error(f"[{symbol}] 数据库操作失败: {db_err}", exc_info=True)
            conn.rollback()
        except Exception as e:
            logger.critical(f"[{symbol}] 交易量对比发生未知异常: {e}", exc_info=True)

    cursor.close()
    logger.info("🎉 15min 累计交易量对比周期完成。")
    logger.info("=" * 70)


def main():
    setup_logging()
    conn = None
    try:
        # 初始化一次连接
        config = load_config()
        conn = init_db(config)
        frequency = 60  # 每 60 秒一次
        logger.info(f"交易量监控脚本已启动，运行频率为每 {frequency} 秒一次...")

        while True:
            # 每轮重载配置，确保改 config.json 能即时生效
            config = load_config()
            logger.info(f"[调试] utils.py 路径: {_u.__file__}")
            logger.info(f"[调试] ALERT_CONFIG keys: {list((config or {}).get('ALERT_CONFIG', {}).keys())}")

            # 额外打印全局容差，帮助确认读取值
            ac = (config or {}).get('ALERT_CONFIG', {}) or {}
            logger.info(f"[全局调试] ALERT_CONFIG.VOLUME_RATIO_THRESHOLD = {ac.get('VOLUME_RATIO_THRESHOLD')}")

            compare_volume_alert(conn, config)
            time.sleep(frequency)

    except KeyboardInterrupt:
        logger.info("用户中断程序 (Ctrl+C)。程序正在退出...")
    except Exception as e:
        logger.critical(f"交易量脚本发生致命错误，正在退出: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭。程序退出。")


if __name__ == '__main__':
    main()
