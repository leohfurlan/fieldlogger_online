"""Central configuration for Oracle access and logging."""

import os
import sys

import oracledb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

ORACLE_CLIENT_LIB_DIR = os.getenv("ORACLE_CLIENT_LIB_DIR")
ORACLE_DRIVER_MODE = "thin"
_mode_pref = (os.getenv("ORACLE_MODE", "auto") or "auto").strip().lower()

if _mode_pref not in {"auto", "thin", "thick"}:
    _mode_pref = "auto"

if _mode_pref == "thin":
    logger.info("Oracle driver inicializado em thin mode (ORACLE_MODE=thin).")
elif _mode_pref == "thick":
    if not ORACLE_CLIENT_LIB_DIR:
        raise RuntimeError(
            "ORACLE_MODE=thick exige ORACLE_CLIENT_LIB_DIR configurado."
        )
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
    ORACLE_DRIVER_MODE = "thick"
    logger.info(
        "Oracle driver inicializado em thick mode (ORACLE_MODE=thick): {}",
        ORACLE_CLIENT_LIB_DIR,
    )
else:
    # auto: tenta thick quando Instant Client estiver configurado; fallback para thin.
    if ORACLE_CLIENT_LIB_DIR:
        try:
            oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
            ORACLE_DRIVER_MODE = "thick"
            logger.info(
                "Oracle driver inicializado em thick mode (auto): {}",
                ORACLE_CLIENT_LIB_DIR,
            )
        except Exception as exc:
            ORACLE_DRIVER_MODE = "thin"
            logger.warning(
                "Falha ao iniciar thick mode ({}); fallback para thin. Motivo: {}",
                ORACLE_CLIENT_LIB_DIR,
                exc,
            )
    else:
        logger.info("Oracle driver inicializado em thin mode (auto).")

DB_DSN = os.getenv("DB_DSN")          # Ex.: //host:1521/ORCLPDB1  (EZCONNECT recomendado p/ thin)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


ENERGY_EST_ENABLED = _env_bool("ENERGY_EST_ENABLED", True)
ENERGY_VLL_VOLTS = _env_float("ENERGY_VLL_VOLTS", 380.0)
ENERGY_POWER_FACTOR = _env_float("ENERGY_POWER_FACTOR", 0.90)
ENERGY_LABEL = _env_str(
    "ENERGY_LABEL",
    "Energia estimada (kWh) — PF assumido",
)

LOG_FILE = os.getenv("APP_LOG_FILE", "app.log")
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO")
LOG_ROTATION = os.getenv("APP_LOG_ROTATION", "10 MB")
LOG_RETENTION = os.getenv("APP_LOG_RETENTION", "14 days")
LOG_COMPRESSION = os.getenv("APP_LOG_COMPRESSION", "zip")

# Replace default sink and keep one console stream + one structured JSON file sink.
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL, backtrace=False, diagnose=False)
logger.add(
    LOG_FILE,
    level=LOG_LEVEL,
    rotation=LOG_ROTATION,
    retention=LOG_RETENTION,
    compression=LOG_COMPRESSION,
    serialize=True,
    enqueue=True,
    backtrace=False,
    diagnose=False,
)

def get_conn():
    """
    Return a new Oracle connection using environment credentials.
    Dica de DSN (thin/EZCONNECT): //host:1521/servicename
    """
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)

__all__ = [
    "DB_DSN",
    "DB_USER",
    "DB_PASSWORD",
    "ORACLE_DRIVER_MODE",
    "ENERGY_EST_ENABLED",
    "ENERGY_VLL_VOLTS",
    "ENERGY_POWER_FACTOR",
    "ENERGY_LABEL",
    "get_conn",
    "logger",
]
