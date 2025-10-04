import time
import logging
from datetime import datetime
import pymysql
import sys
import os
from typing import Union

# 导入公共工具和告警模块
# 假设 utils.py, lark_alerter.py 存在
try:
    from utils import load_config, init_db, get_threshold
except ImportError:
    print("FATAL ERROR: 缺少 utils.py 文件，无法导入配置和DB连接函数。")
    sys.exit(1)

try:
    from lark_alerter import send_lark_alert
except ImportError:
    def send_lark_alert(*args, **kwargs):
        logging.getLogger('monitor_system').error("Lark告警模块未找到，无法发送通知。")

# 脚本启动前获取 logger 实例名
logger = logging.getLogger('monitor_system')


# --- 日志配置函数 ---
def setup_logging():
    """
    配置日志系统，确保日志同时输出到控制台 (StreamHandler) 和文件 (FileHandler)。
    日志文件名为 volume_monitor_log.log，存放在脚本同一路径下。
    """
    logger_name = 'monitor_system'
    root_logger = logging.getLogger(logger_name)

    # 仅在未配置 Handler 时进行初始化，防止重复日志
    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)

        # 1. 定义日志格式：增加文件名和行号，方便追溯
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 2. 创建 StreamHandler (输出到控制台)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        # 3. 创建 FileHandler (输出到文件：volume_monitor_log.log)
        log_dir = os.path.dirname(os.path.abspath(__file__))
        log_file_path = os.path.join(log_dir, 'volume_monitor_log.log')

        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)

        # 4. 将 Handler 添加到 Logger
        root_logger.addHandler(ch)
        root_logger.addHandler(fh)


# --- 辅助函数：将时间戳或 datetime 对象转换为易读格式 ---
def format_timestamp(ts: Union[int, float, datetime, None]) -> str:
    """
    将时间戳（毫秒或 datetime 对象）转换为 YYYY-MM-DD HH:MM 格式的本地时间。

    参数 ts: 可以是毫秒时间戳 (int/float)、datetime.datetime 对象或 None。
    """
    if ts is None:
        return "N/A"

    dt_object = None

    if isinstance(ts, datetime):
        # 如果已经是 datetime 对象，直接使用
        dt_object = ts
    elif isinstance(ts, (int, float)):
        # 如果是数字（毫秒时间戳），转换为秒
        ts_s = ts / 1000
        # 转换为本地时间
        dt_object = datetime.fromtimestamp(ts_s)
    else:
        # 如果传入的不是预期类型，记录错误并返回默认值
        logger.error(f"format_timestamp 收到不支持的类型: {type(ts)}")
        return "类型错误"

    # 如果转换成功，进行格式化
    return dt_object.strftime('%Y-%m-%d %H:%M')


# --- 交易量对比逻辑 (基于 15min 累计) ---

def compare_volume_alert(conn, config):
    """从数据库获取 15min 累计交易量数据，并执行对比告警。"""

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
        # 初始化时间戳范围变量
        formatted_time_start = "N/A"
        formatted_time_end = "N/A"
        timestamps_to_compare = set()  # 确保在 try 块外部也能访问，即使为空

        try:
            symbol_conf = config['ALERT_CONFIG'].get('SYMBOL_THRESHOLDS', {}).get(symbol, {})

            # --- 1. 数据查询 ---
            # 查询目标平台 (A) 最新的 15 条 K 线
            sql_a = f"""
                SELECT timestamp, volume FROM {table_names['KLINE_DATA']} 
                WHERE symbol = %s AND exchange = %s
                ORDER BY timestamp DESC
                LIMIT 15 
            """
            cursor.execute(sql_a, (symbol, A_ID))
            rows_a = cursor.fetchall()

            # 查询基准平台 (B) 最新的 15 条 K 线
            sql_b = f"""
                SELECT timestamp, volume FROM {table_names['KLINE_DATA']} 
                WHERE symbol = %s AND exchange = %s
                ORDER BY timestamp DESC
                LIMIT 15 
            """
            cursor.execute(sql_b, (symbol, B_ID))
            rows_b = cursor.fetchall()

            # --- 2. K线对齐并计算累计交易量 ---
            A_15min_volume, B_15min_volume = 0.0, 0.0

            if len(rows_a) < 15 or len(rows_b) < 15:
                # 即使数据不足，也尝试获取最新的时间点进行日志记录
                latest_ts_value = rows_a[0]['timestamp'] if rows_a else None
                formatted_time_end = format_timestamp(latest_ts_value)

                logger.warning(
                    f"[{formatted_time_end}][{symbol}] ⚠️ 数据量不足：{A_ID} ({len(rows_a)}条) 或 {B_ID} ({len(rows_b)}条) K线不足 15 条，跳过对比。"
                )
            else:
                vol_map_a = {r['timestamp']: r['volume'] for r in rows_a}
                vol_map_b = {r['timestamp']: r['volume'] for r in rows_b}

                # 🚨 关键：时间戳交集操作
                timestamps_to_compare = set(vol_map_a.keys()) & set(vol_map_b.keys())

                if len(timestamps_to_compare) < 10:
                    # 即使对齐失败，也使用最新的时间点进行日志记录
                    latest_ts_value = rows_a[0]['timestamp']
                    formatted_time_end = format_timestamp(latest_ts_value)

                    logger.warning(
                        f"[{formatted_time_end}][{symbol}] ⚠️ 对齐失败：两个平台共同 K线时间戳不足 10 条 ({len(timestamps_to_compare)}条)，跳过对比。"
                    )
                else:
                    # 🚨 提取时间戳范围
                    min_ts = min(timestamps_to_compare)
                    max_ts = max(timestamps_to_compare)

                    formatted_time_end = format_timestamp(max_ts)
                    formatted_time_start = format_timestamp(min_ts)

                    A_15min_volume = sum(vol_map_a[ts] for ts in timestamps_to_compare)
                    B_15min_volume = sum(vol_map_b[ts] for ts in timestamps_to_compare)

                    logger.info(
                        f"[{formatted_time_end}][{symbol}] ✅ 数据对齐成功。共同 K 线数量: {len(timestamps_to_compare)} 条。")

            # --- 3. 对比和告警逻辑（使用累计交易量） ---
            A_volume = A_15min_volume
            B_volume = B_15min_volume

            volume_ratio_threshold = get_threshold(config, symbol_conf, 'VOLUME_RATIO_THRESHOLD', 0.1)
            time_range_str = f"{formatted_time_start} -> {formatted_time_end}"  # 组合时间范围字符串

            logger.info("-" * 70)
            logger.info(f"--- 📊 {time_range_str} | {symbol} 交易量报告 ({A_ID} vs {B_ID}) ---")
            logger.info(f"交易量对比详情 (基于 {len(timestamps_to_compare)} 条共同 K线累计):")

            vol_log_lines = []
            vol_log_lines.append(
                f"{'项':<5} | {A_ID:<12} | {B_ID:<12} | {'差值':<10} | {'比例':<8} | {'阈值':<8} | {'结果'}")
            vol_log_lines.append("-" * 70)

            result_status = "---"
            ratio_str = "N/A"
            diff_str = "N/A"

            if B_volume > 0 and A_volume >= 0:
                volume_ratio = A_volume / B_volume
                diff = A_volume - B_volume
                ratio_str = f"{volume_ratio:.4f}"
                diff_str = f"{diff:+.2f}"

                if volume_ratio < volume_ratio_threshold:
                    result_status = "🚨 告警"
                    title = f"🚨 流动性不足告警: {symbol} @ {formatted_time_end} (15min 累计)"
                    text = (
                        f"**K线时间范围**: {time_range_str}\n"
                        f"平台A 15min 累计成交量低于币安阈值。\n"
                        f"平台 A ({A_ID}) 累计量: {A_volume:.2f}\n"
                        f"平台 B ({B_ID}) 累计量: {B_volume:.2f}\n"
                        f"成交量比例: {volume_ratio:.2f} (阈值: {volume_ratio_threshold:.2f})"
                    )
                    send_lark_alert(lark_app_config, title, text)
                else:
                    result_status = "✅ 正常"
            elif B_volume == 0 and A_volume > 0:
                result_status = "❌ 异常"
                ratio_str = "Inf"
                diff_str = f"+{A_volume:.2f}"
                logger.error(
                    f"[{formatted_time_end}][{symbol}] 基准平台 {B_ID} 15min 累计交易量为 0，目标平台 {A_ID} 交易量不为 0。")
            elif B_volume == 0 and A_volume == 0:
                result_status = "静止"
                ratio_str = "0.0000"
                diff_str = "0.00"
                logger.info(f"[{formatted_time_end}][{symbol}] 两个平台 15min 累计交易量均为 0，数据静止。")
            else:
                result_status = "数据异常"
                logger.error(
                    f"[{formatted_time_end}][{symbol}] 出现负值交易量或其它逻辑错误。A:{A_volume}, B:{B_volume}")

            # 记录交易量对比结果
            vol_log_lines.append(
                f"{'Volume':<5} | {A_volume:<12.2f} | {B_volume:<12.2f} | {diff_str:<10} | {ratio_str:<8} | {volume_ratio_threshold:<8.2f} | {result_status}"
            )

            # 输出所有日志行
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
    # 🚨 设置日志系统，确保文件日志和控制台输出
    setup_logging()

    conn = None
    try:
        # 加载配置和连接 DB
        config = load_config()
        conn = init_db(config)

        frequency = 60  # 每 60 秒运行一次

        logger.info(f"交易量监控脚本已启动，运行频率为每 {frequency} 秒一次...")

        while True:
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