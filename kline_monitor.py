import pymysql
from pymysql import cursors
import time
# 🚨 使用 json5 库来支持配置文件的注释
import json5 as json
import logging
from datetime import datetime
import sys
import os
from datetime import datetime, timedelta

# 导入自定义模块
try:
    from platform_connector import PlatformConnector, OHLCV_INDEX
    from lark_alerter import send_lark_alert
except ImportError as e:
    print(f"致命错误: 无法导入自定义模块。请确保 platform_connector.py 和 lark_alerter.py 存在。错误: {e}")
    sys.exit(1)

# --- 日志系统增强配置 ---
LOG_FILE = 'monitor_system.log'

# 修正：统一使用 'monitor_system' 作为 logger 名称，并设置最低级别
logger = logging.getLogger('monitor_system')
logger.setLevel(logging.DEBUG)  # 确保最低级别 DEBUG 被设置

# 防止重复添加 Handler
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

# FileHandler：写入文件 (DEBUG 级别)
file_handler = logging.FileHandler(os.path.join(os.getcwd(), LOG_FILE), encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler：输出终端 (INFO 级别)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)  # 终端仍输出 INFO 及以上
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# --- 日志系统增强配置结束 ---


# --- 1. 配置加载与数据库连接 ---

CONFIG_FILE_PATH = 'config.json'


def load_config():
    """从 config.json 文件中加载所有配置 (使用 json5 兼容注释)"""
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info("配置文件加载成功。")
        return config
    except Exception as e:
        logger.critical(f"致命错误: 无法加载配置文件或配置文件格式错误: {e}")
        raise


def init_db(config):
    """建立 MySQL 数据库连接"""
    mysql_conf = config['MYSQL_CONFIG']
    try:
        conn = pymysql.connect(
            host=mysql_conf['HOST'],
            user=mysql_conf['USER'],
            password=mysql_conf['PASSWORD'],
            database=mysql_conf['DATABASE'],
            port=mysql_conf['PORT'],
            cursorclass=cursors.Cursor,
            autocommit=False
        )
        logger.info("MySQL 数据库连接成功。")
        return conn
    except Exception as e:
        logger.critical(f"MySQL 连接失败: 请检查配置和数据库服务。错误: {e}")
        raise


# --- 2. 断点续传和去重逻辑 ---

def get_last_kline_time_exact(conn, symbol, exchange_id, table_name):
    """查询数据库，获取指定交易所和合约的最新 K 线时间戳（datetime对象）。"""
    cursor = conn.cursor()
    sql = f"""
        SELECT timestamp 
        FROM {table_name} 
        WHERE symbol = %s AND exchange = %s 
        ORDER BY timestamp DESC 
        LIMIT 1
    """
    try:
        cursor.execute(sql, (symbol, exchange_id.upper()))
        result = cursor.fetchone()

        if result:
            return result[0]  # 返回 datetime 对象

    except Exception as e:
        logger.warning(f"获取 {exchange_id} {symbol} 上次时间失败: {e}")
    finally:
        cursor.close()

    return None


# --- 3. 数据获取与存储 (数据管道) ---

def fetch_and_store_data(conn, config):
    """循环所有合约和交易所，获取 K线和费率数据并批量写入数据库"""

    symbols = config['MONITORED_SYMBOLS']
    ex_conf = config['EXCHANGE_CONFIG']
    table_names = config['TABLE_NAMES']

    # 初始化连接器
    platform_a = PlatformConnector(ex_conf['PLATFORM_A_ID'], ex_conf['PLATFORM_A_API_URL'])
    binance = PlatformConnector(ex_conf['BENCHMARK_ID'], ex_conf['BINANCE_API_URL'])
    connectors = {ex_conf['PLATFORM_A_ID']: platform_a, ex_conf['BENCHMARK_ID']: binance}

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    kline_inserts = []

    logger.info(f"[{now_ts}] 开始获取 {len(symbols)} 个合约的数据...")

    # 遍历所有连接器和合约
    for platform_id, connector in connectors.items():
        for symbol in symbols:

            last_kline_dt = get_last_kline_time_exact(conn, symbol, platform_id, table_names['KLINE_DATA'])

            # --- 断点和启动逻辑 ---

            start_time_ms = None
            kline_count = 5  # 默认抓取条数

            if last_kline_dt:
                # 数据库有数据：从最新时间前 1 毫秒开始，确保最新一条能被 API 重新返回，依赖去重
                start_time_ms = int(last_kline_dt.timestamp() * 1000) - 1
            else:
                # 数据库为空：利用 start_time 限制回传数量到 5 条
                time_frame_minutes = 1
                rollback_minutes = kline_count * time_frame_minutes + 1
                now_time = datetime.now().replace(second=0, microsecond=0)
                target_start_dt = now_time - timedelta(minutes=rollback_minutes)
                start_time_ms = int(target_start_dt.timestamp() * 1000)

                logger.info(
                    f"[{platform_id}][{symbol}] 数据库为空，将从 {target_start_dt.strftime('%H:%M')} 开始获取数据，"
                    f"预计获取约 {rollback_minutes} 条数据作为启动点。"
                )

            # API 调用
            klines = connector.fetch_ohlcv_history(
                symbol,
                ex_conf['TIME_FRAME'],
                start_time_ms=start_time_ms,
            )

            # 确保 klines 是一个列表，避免 NoneType 错误
            if klines and isinstance(klines, list):

                # 处理 BITDA 的双层嵌套结构
                final_klines_list = klines
                if platform_id == 'BITDA_FUTURES':
                    if klines and isinstance(klines[0], list):
                        final_klines_list = klines[0]

                new_data_count = 0

                # 首次启动且数据量大于 kline_count，进行截断
                if not last_kline_dt and platform_id == 'BITDA_FUTURES' and len(final_klines_list) > kline_count + 2:
                    final_klines_list = final_klines_list[-(kline_count + 2):]
                    logger.warning(
                        f"[{platform_id}][{symbol}] API 首次返回数据量过大 ({len(klines)}条)，已手动截断至 {len(final_klines_list)} 条。"
                    )

                for kline in final_klines_list:  # 遍历解开后的列表

                    # 分平台解析 K线数据
                    try:
                        if platform_id == 'BITDA_FUTURES':
                            # BITDA 格式：字典键名 (Key)
                            timestamp_ms = kline['time']
                            o = float(kline['open'])
                            h = float(kline['high'])
                            l = float(kline['low'])
                            c = float(kline['close'])
                            volume = float(kline['volume'])
                            quote_volume = 0.0

                        else:
                            # BINANCE/标准格式：索引 (Index)
                            timestamp_ms = kline[OHLCV_INDEX['timestamp']]
                            o = kline[OHLCV_INDEX['open']]
                            h = kline[OHLCV_INDEX['high']]
                            l = kline[OHLCV_INDEX['low']]
                            c = kline[OHLCV_INDEX['close']]
                            volume = kline[OHLCV_INDEX['volume']]
                            quote_volume = kline[OHLCV_INDEX['quote_volume']]

                        kline_dt_api = datetime.fromtimestamp(timestamp_ms / 1000)

                    except (KeyError, ValueError, TypeError) as e:
                        logger.error(f"[{platform_id}][{symbol}] K线数据解析失败: {e}. 原始数据: {kline}")
                        continue

                        # 检查数据是否已在数据库中 (去重逻辑)
                    if last_kline_dt and kline_dt_api <= last_kline_dt:
                        if kline_dt_api == last_kline_dt:
                            logger.warning(
                                f"[{platform_id}][{symbol}] API数据未更新! "
                                f"最新记录时间 {kline_dt_api.strftime('%Y-%m-%d %H:%M:%S')} 已存在于DB中。"
                            )
                        continue

                        # 格式化时间戳，用于写入数据库
                    kline_time_minute = kline_dt_api.strftime('%Y-%m-%d %H:%M')
                    kline_time = f"{kline_time_minute}:00"

                    # K线一字线判断逻辑
                    is_one_line = 1 if (o == h and h == l and l == c) else 0

                    kline_inserts.append((
                        kline_time, platform_id.upper(), symbol,
                        o, h, l, c,
                        volume, quote_volume,
                        is_one_line
                    ))
                    new_data_count += 1

                logger.info(f"[{platform_id}][{symbol}] 准备写入 {new_data_count} 条 K线新数据。")

    # --- 批量写入 MySQL 数据库 ---
    cursor = conn.cursor()
    try:
        kline_sql = f"""
            INSERT IGNORE INTO {table_names['KLINE_DATA']} 
            (timestamp, exchange, symbol, open, high, low, close, volume, quote_volume, is_one_line)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        if kline_inserts:
            cursor.executemany(kline_sql, kline_inserts)

        conn.commit()
        logger.info(f"数据写入数据库完成。K线新数据: {len(kline_inserts)} 条。")

    except Exception as e:
        conn.rollback()
        logger.error(f"MySQL 批量写入失败，已回滚: {e}")
    finally:
        cursor.close()


# --- 4. 数据对比与告警逻辑 (默认开启比对) ---

# --- 4. 数据对比与告警逻辑 (默认开启比对) ---

# --- 4. 数据对比与告警逻辑 (默认开启比对) ---

def compare_and_alert(conn, config):
    """从数据库获取最新数据，执行跨交易所对比逻辑，并发送告警。默认执行所有对比。"""

    symbols = config['MONITORED_SYMBOLS']
    ex_conf = config['EXCHANGE_CONFIG']
    alert_conf = config['ALERT_CONFIG']
    table_names = config['TABLE_NAMES']
    lark_app_config = config['LARK_APP_CONFIG']

    A_ID = ex_conf['PLATFORM_A_ID'].upper()
    B_ID = ex_conf['BENCHMARK_ID'].upper()

    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    logger.info("=" * 60)
    logger.info("开始执行跨交易所数据对比 (成交量对比已禁用)...") # ⚠️ 提示用户成交量已禁用

    # --- 辅助函数：获取阈值 ---
    def get_threshold(symbol_conf, key, default):
        """从配置中获取指定合约或全局的阈值"""
        if symbol_conf and key in symbol_conf:
            return symbol_conf[key]
        return alert_conf.get(key, default)

    for symbol in symbols:
        symbol_conf = alert_conf.get('SYMBOL_THRESHOLDS', {}).get(symbol, {})

        # 4.1 K线数据获取和时间戳对齐逻辑 (保持不变)
        sql = f"""
            SELECT * FROM {table_names['KLINE_DATA']} 
            WHERE symbol = %s AND (exchange = %s OR exchange = %s)
            ORDER BY timestamp DESC
            LIMIT 4 
        """
        cursor.execute(sql, (symbol, A_ID, B_ID))
        all_latest_rows = cursor.fetchall()

        if len(all_latest_rows) < 2:
            logger.warning(f"[{symbol}] 数据库数据不足 (少于2条)，跳过对比。")
            continue

        data_a = next((r for r in all_latest_rows if r['exchange'] == A_ID), None)
        data_b = next((r for r in all_latest_rows if r['exchange'] == B_ID), None)

        if not data_a or not data_b:
            logger.warning(f"[{symbol}] 缺失 {A_ID} 或 {B_ID} 的最新数据，跳过对比。")
            continue

        # 强制时间戳对齐检查 (逻辑保持不变)
        timestamp_a = data_a['timestamp']
        timestamp_b = data_b['timestamp']

        if timestamp_a != timestamp_b:
            target_timestamp = min(timestamp_a, timestamp_b)

            cursor.execute(f"""
                SELECT * FROM {table_names['KLINE_DATA']} 
                WHERE symbol = %s AND timestamp = %s AND exchange = %s 
            """, (symbol, target_timestamp, A_ID))
            data_a_aligned = cursor.fetchone()

            cursor.execute(f"""
                SELECT * FROM {table_names['KLINE_DATA']} 
                WHERE symbol = %s AND timestamp = %s AND exchange = %s 
            """, (symbol, target_timestamp, B_ID))
            data_b_aligned = cursor.fetchone()

            if not data_a_aligned or not data_b_aligned:
                logger.warning(
                    f"[{symbol}] 最新两条K线时间戳不一致，且无法对齐到较早时间戳。跳过对比。"
                )
                continue

            data_a = data_a_aligned
            data_b = data_b_aligned
            logger.warning(
                f"[{symbol}] 最新K线时间戳不一致，已自动对齐到 {target_timestamp.strftime('%H:%M:%S')} 进行比对。"
            )

        kline_time = data_a['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        logger.info("-" * 60)
        logger.info(f"--- 📊 {symbol} 对比报告 @ {kline_time} ---")

        # --- A. K线价格偏离对比 (保持不变) ---
        price_fields = ['open', 'high', 'low', 'close']

        price_log_lines = [
            f"{'价格项':<5} | {A_ID:<10} | {B_ID:<10} | {'差值':<10} | {'比例':<8} | {'结果'}"
        ]
        price_log_lines.append("-" * 60)

        # 价格对比：默认执行
        for field in price_fields:
            A_price = data_a.get(field, 0.0)
            B_price = data_b.get(field, 0.0)

            result_status = "---"
            deviation_str = "N/A"
            diff_str = "N/A"

            # 根据字段动态获取对应的阈值键名
            threshold_key = f"{field.upper()}_DEVIATION_THRESHOLD"
            default_threshold = alert_conf.get(threshold_key, 0.005)
            price_threshold = get_threshold(symbol_conf, threshold_key, default_threshold)

            if B_price > 0 and A_price >= 0:
                deviation = abs(A_price - B_price) / B_price
                diff = A_price - B_price

                diff_str = f"{diff:+.6f}"
                deviation_str = f"{deviation * 100:.4f}%"

                if deviation > price_threshold:
                    result_status = "🚨 告警"
                    title = f"🚨 K线价格偏离告警: {symbol} / {field.upper()} @ {kline_time}"
                    text = (
                        f"【{field.upper()}】价格偏离超过阈值。\n"
                        f"平台 A ({A_ID}): {A_price}\n"
                        f"平台 B ({B_ID}): {B_price}\n"
                        f"偏离度: {deviation:.4f} (阈值: {price_threshold * 100:.2f}%)"
                    )
                    send_lark_alert(lark_app_config, title, text)
                else:
                    result_status = "✅ 正常"

            price_log_lines.append(
                f"{field.upper():<5} | {A_price:<10.6f} | {B_price:<10.6f} | {diff_str:<10} | {deviation_str:<8} | {result_status}"
            )

        logger.info("价格对比详情:")
        for line in price_log_lines:
            logger.info(line)

        # --- B. 成交量异常对比 (已移除) ---

        # --- D. K线一字线告警 (默认开启) ---
        # 🚨 修正：一字线检查紧随价格对比日志后，用分隔线隔开
        logger.info("-" * 60)

        # 使用 ONE_LINE_KLINE_COUNT 键名，默认 2 条
        max_count = get_threshold(symbol_conf, 'ONE_LINE_KLINE_COUNT', 2)

        # 1. 修正 SQL：查询最新的 MAX_COUNT + 5 条 K线数据
        check_limit = max_count + 5

        one_line_sql = f"""
                    SELECT is_one_line 
                    FROM {table_names['KLINE_DATA']}
                    WHERE exchange = %s AND symbol = %s 
                    ORDER BY timestamp DESC
                    LIMIT %s
                """
        # 使用 check_limit 限制查询数量
        cursor.execute(one_line_sql, (A_ID, symbol, check_limit))

        # 2. 关键修正：使用字典键 'is_one_line' 访问数据
        one_line_flags = [row['is_one_line'] for row in cursor.fetchall()]

        # 3. Python 侧检查连续性
        continuous_count = 0
        for flag in one_line_flags:
            if flag == 1:
                continuous_count += 1
            else:
                break

        log_msg = f"[{symbol}] 一字线检查: 连续 {continuous_count} 条 (阈值: {max_count} 条)."

        # 4. 告警逻辑
        if continuous_count >= max_count and continuous_count > 0:
            logger.critical(f"❗❗ {log_msg} -> 触发告警！")
            title = f"❗ K线异常告警: {symbol} 连续一字线 ({continuous_count} 条)"
            text = (
                f"平台 {A_ID} 的 {symbol} 连续 {continuous_count} 个周期出现一字线。\n"
                f"这可能意味着数据流停滞或交易异常。 (阈值: {max_count} 条)"
            )
            send_lark_alert(lark_app_config, title, text)
        else:
            logger.info(f"✅ {log_msg}")

    cursor.close()
    logger.info("跨交易所数据对比结束。")
    logger.info("=" * 60)


# --- 5. 主执行逻辑 ---
def main():
    conn = None
    try:
        config = load_config()

        # 🚨 强制配置诊断代码（保留）：用于确认配置加载是否正常
        alert_conf = config.get('ALERT_CONFIG', {})
        print("\n" + "=" * 50)
        print("--- 🚨 强制配置诊断输出 ---")
        # 即使开关被移除，我们依然打印出它们的值，作为调试参考
        print(
            f"KLINE_PRICE_ALERT_ENABLED: {alert_conf.get('KLINE_PRICE_ALERT_ENABLED', 'MISSING')} (Type: {type(alert_conf.get('KLINE_PRICE_ALERT_ENABLED'))})")
        print(
            f"VOLUME_ALERT_ENABLED:      {alert_conf.get('VOLUME_ALERT_ENABLED', 'MISSING')} (Type: {type(alert_conf.get('VOLUME_ALERT_ENABLED'))})")
        print(
            f"ONE_LINE_KLINE_ALERT_ENABLED: {alert_conf.get('ONE_LINE_KLINE_ALERT_ENABLED', 'MISSING')} (Type: {type(alert_conf.get('ONE_LINE_KLINE_ALERT_ENABLED'))})")
        print("--------------------------")

        conn = init_db(config)

        # 获取 LARK APP 配置
        lark_app_config = config['LARK_APP_CONFIG']

        send_lark_alert(lark_app_config,
                        "✅ 监控脚本启动",
                        f"系统开始监控。")

        # 从配置中获取执行间隔时间（秒）
        frequency = config['EXCHANGE_CONFIG']['FREQUENCY_SECONDS']

        logger.info(f"监控脚本已启动，运行频率为每 {frequency} 秒一次...")

        while True:
            # 1. 执行数据获取和存储
            fetch_and_store_data(conn, config)

            # 2. 执行数据对比和告警
            compare_and_alert(conn, config)

            # 3. 暂停，等待下一轮执行
            time.sleep(frequency)

    except Exception as e:
        logger.critical(f"脚本发生致命错误，正在退出: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭。程序退出。")


if __name__ == '__main__':
    main()