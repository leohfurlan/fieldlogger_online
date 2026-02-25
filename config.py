"""Central configuration for Oracle access and logging."""

import os
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

# Structured JSON logs persisted to file
LOG_FILE = os.getenv("APP_LOG_FILE", "app.log")
logger.add(LOG_FILE, rotation="1 MB", serialize=True)

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
