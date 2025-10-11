# utils.py
import pymysql
from pymysql import cursors
import json5 as json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger('monitor_system')

# ✅ 固定读取 utils.py 同目录下的 config.json；也支持通过环境变量覆盖
CONFIG_FILE_PATH = Path(
    (Path(os.getenv("CONFIG_FILE_PATH")).expanduser() if os.getenv("CONFIG_FILE_PATH") else Path(__file__).resolve().parent / "config.json")
).resolve()

def load_config():
    """从 config.json 文件中加载所有配置 (使用 json5 兼容注释)"""
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # ✅ 打印本次读取的配置路径与关键键，便于核对版本与内容
        ac = (config or {}).get('ALERT_CONFIG', {}) or {}
        logger.info(f"[CONFIG] 已读取配置文件: {CONFIG_FILE_PATH}")
        logger.info(f"[CONFIG] ALERT_CONFIG keys: {list(ac.keys())}")

        return config
    except FileNotFoundError:
        logger.critical(f"找不到配置文件: {CONFIG_FILE_PATH}")
        raise
    except Exception as e:
        logger.critical(f"加载配置失败: {e}")
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

def get_threshold(config, symbol_conf, key, default):
    """从配置中获取指定合约或全局的阈值"""
    alert_conf = config['ALERT_CONFIG']
    if symbol_conf and key in symbol_conf:
        return symbol_conf[key]
    return alert_conf.get(key, default)
