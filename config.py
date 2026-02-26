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
    "get_conn",
    "logger",
]
