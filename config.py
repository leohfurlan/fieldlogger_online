"""Central configuration for Oracle access and logging."""

import os
import oracledb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# --- Oracle Client (thick) com fallback para thin ---
LIB_DIR = os.getenv("ORACLE_CLIENT_LIB_DIR")
if LIB_DIR:
    try:
        oracledb.init_oracle_client(lib_dir=LIB_DIR)
        logger.info(f"Oracle Client carregado (thick mode) em: {LIB_DIR}")
    except Exception as e:
        # Falhou carregar o client nativo -> segue em thin mode
        logger.warning(f"Falha ao iniciar Oracle Client ({LIB_DIR}); usando thin mode. Motivo: {e}")
else:
    logger.info("Sem ORACLE_CLIENT_LIB_DIR; usando thin mode (sem Instant Client).")

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

__all__ = ["get_conn", "logger"]
