import pymysql
from pymysql import cursors
import json5 as json
import logging
import sys

# 假设 logger 在主脚本中已配置好，这里直接获取
logger = logging.getLogger('monitor_system')
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


def get_threshold(config, symbol_conf, key, default):
    """从配置中获取指定合约或全局的阈值"""
    alert_conf = config['ALERT_CONFIG']
    if symbol_conf and key in symbol_conf:
        return symbol_conf[key]
    return alert_conf.get(key, default)