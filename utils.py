# utils.py
import logging
from pathlib import Path

logger = logging.getLogger('monitor_system')

# ---- TOML loader ----
try:
    import tomllib as toml  # Python 3.11+
except Exception:
    import tomli as toml    # pip install tomli

ROOT = Path(__file__).resolve().parent
CFG_MAIN  = ROOT / "config.toml"             # 固定读取脚本同目录
CFG_LOCAL = ROOT / "config.local.toml"       # 可选：存在则覆盖差异

def _load_toml(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "rb") as f:
        return toml.load(f)

def _deep_merge(base: dict, extra: dict) -> dict:
    if not isinstance(extra, dict):
        return base
    out = dict(base)
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def load_config() -> dict:
    """热加载配置：config.toml + config.local.toml（后者覆盖前者，若存在）"""
    main_cfg  = _load_toml(CFG_MAIN)
    local_cfg = _load_toml(CFG_LOCAL)
    config = _deep_merge(main_cfg, local_cfg)

    ac = (config or {}).get("ALERT_CONFIG", {}) or {}
    logger.info(f"[CONFIG] 读取: {CFG_MAIN} (+{CFG_LOCAL.name if CFG_LOCAL.exists() else '无覆盖'})")
    logger.info(f"[CONFIG] ALERT_CONFIG keys: {list(ac.keys())}")
    return config

# ---- DB ----
import pymysql
from pymysql import cursors

def init_db(config: dict):
    """建立 MySQL 数据库连接"""
    mysql_conf = config['MYSQL_CONFIG']
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

def get_threshold(config, symbol_conf, key, default):
    """保持与旧代码兼容的阈值读取：优先 symbol 覆盖，其次全局 ALERT_CONFIG"""
    alert_conf = (config or {}).get('ALERT_CONFIG', {}) or {}
    if symbol_conf and key in symbol_conf:
        return symbol_conf[key]
    return alert_conf.get(key, default)
