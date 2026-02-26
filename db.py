# app/db.py
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

import oracledb

from config import DB_DSN, DB_PASSWORD, DB_USER, ORACLE_DRIVER_MODE, logger

MONITOR_OWNER = "ENGENHARIA"
MONITOR_TABLE = "FIELDLOGGER_MONITOR"
MONITOR_TABLE_FQN = f"{MONITOR_OWNER}.{MONITOR_TABLE}"
BATCH_INDEX_NAME = "IX_FL_MONITOR_BATCH_ID"
MONITOR_BATCH_FK_NAME = "FK_FL_MONITOR_BATCHES"
COMPOSTO_COD_INDEX_NAME = "IX_FL_MONITOR_COMP_COD"
COMPOSTOS_TABLE = "FIELDLOGGER_COMPOSTOS"
COMPOSTOS_TABLE_FQN = f"{MONITOR_OWNER}.{COMPOSTOS_TABLE}"
COMPOSTOS_INDEX_DESC = "IX_FL_COMPOSTOS_DESC"
BATCHES_TABLE = "FIELDLOGGER_BATCHES"
BATCHES_TABLE_FQN = f"{MONITOR_OWNER}.{BATCHES_TABLE}"
BATCHES_OP_INDEX_NAME = "IX_FL_BATCHES_OP"
BATCHES_START_TS_INDEX_NAME = "IX_FL_BATCHES_START_TS"
BATCHES_END_TS_INDEX_NAME = "IX_FL_BATCHES_END_TS"
BATCH_ID_SEQUENCE = "SEQ_BATCH_ID"
BATCH_ID_SEQUENCE_FQN = f"{MONITOR_OWNER}.{BATCH_ID_SEQUENCE}"
APP_STATE_TABLE = "APP_STATE"
APP_STATE_TABLE_FQN = f"{MONITOR_OWNER}.{APP_STATE_TABLE}"
STATE_KEY_CYCLE_DETECTOR = "cycle_detector_state"

LOTE_COLUMN_CANDIDATES = ("LOTE", "LOTE_COMPOSTO", "LOTE_BANBURY")
COMPOSTO_COLUMN_CANDIDATES = ("COMPOSTO", "COMPOSTO_ID", "COD_COMPOSTO", "RECEITA")
BATCH_ID_COLUMN_CANDIDATES = ("BATCH_ID", "BATCH", "ID_BATCH", "BATELADA")
OP_COLUMN_CANDIDATES = ("OP", "ORDEM_PRODUCAO", "ORDEM_PROD", "ORDEM")
OBSERVACOES_COLUMN_CANDIDATES = ("OBSERVACOES", "OBSERVACAO", "OBS", "ANOTACAO")

_MONITOR_COLUMNS_CACHE: set[str] | None = None
_POOL = None
_POOL_LOCK = threading.Lock()


def _env_int(name: str, default: int) -> int:
    text = os.getenv(name)
    if text is None:
        return default
    text = text.strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL

    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL

        if not DB_DSN or not DB_USER or not DB_PASSWORD:
            raise RuntimeError(
                "Credenciais Oracle ausentes. Configure DB_DSN, DB_USER e DB_PASSWORD no .env."
            )

        _POOL = oracledb.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            min=_env_int("DB_POOL_MIN", 1),
            max=_env_int("DB_POOL_MAX", 4),
            increment=_env_int("DB_POOL_INCREMENT", 1),
            timeout=_env_int("DB_POOL_TIMEOUT_S", 60),
            wait_timeout=_env_int("DB_POOL_WAIT_TIMEOUT_MS", 5000),
            getmode=oracledb.POOL_GETMODE_WAIT,
        )
        logger.info("Oracle connection pool criado em {} mode.", ORACLE_DRIVER_MODE)

    return _POOL


@contextmanager
def get_conn():
    """
    Retorna conexão reaproveitada via pool Oracle.
    O close() devolve a sessão para o pool.
    """
    conn = _get_pool().acquire()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def close_pool() -> None:
    global _POOL
    with _POOL_LOCK:
        pool = _POOL
        _POOL = None
    if pool is not None:
        try:
            pool.close(force=True)
        except TypeError:
            pool.close()


def list_tables(owner: str = MONITOR_OWNER):
    sql = """
    SELECT table_name
    FROM all_tables
    WHERE owner = :owner
    ORDER BY table_name
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, owner=owner.upper())
            return [r[0] for r in cur.fetchall()]


def _fetch_monitor_columns(cur, refresh: bool = False) -> set[str]:
    global _MONITOR_COLUMNS_CACHE
    if _MONITOR_COLUMNS_CACHE is not None and not refresh:
        return _MONITOR_COLUMNS_CACHE

    cur.execute(
        """
        SELECT column_name
        FROM all_tab_cols
        WHERE owner = :owner
          AND table_name = :table_name
        """,
        owner=MONITOR_OWNER,
        table_name=MONITOR_TABLE,
    )
    _MONITOR_COLUMNS_CACHE = {r[0] for r in cur.fetchall()}
    return _MONITOR_COLUMNS_CACHE


def _first_existing_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for col in candidates:
        if col in columns:
            return col
    return None


def _detect_optional_filter_columns(cur) -> dict[str, str | None]:
    existing = _fetch_monitor_columns(cur)
    return {
        "lote": _first_existing_column(existing, LOTE_COLUMN_CANDIDATES),
        "composto": _first_existing_column(existing, COMPOSTO_COLUMN_CANDIDATES),
        "batch_id": _first_existing_column(existing, BATCH_ID_COLUMN_CANDIDATES),
        "op": _first_existing_column(existing, OP_COLUMN_CANDIDATES),
        "observacoes": _first_existing_column(existing, OBSERVACOES_COLUMN_CANDIDATES),
    }


def _table_exists(cur, owner: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM all_tables
        WHERE owner = :owner
          AND table_name = :table_name
        """,
        owner=owner,
        table_name=table_name,
    )
    return int(cur.fetchone()[0]) > 0


def _constraint_exists(cur, owner: str, constraint_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM all_constraints
        WHERE owner = :owner
          AND constraint_name = :constraint_name
        """,
        owner=owner,
        constraint_name=constraint_name,
    )
    return int(cur.fetchone()[0]) > 0


def _sequence_exists(cur, owner: str, sequence_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM all_sequences
        WHERE sequence_owner = :owner
          AND sequence_name = :sequence_name
        """,
        owner=owner,
        sequence_name=sequence_name,
    )
    return int(cur.fetchone()[0]) > 0


def ensure_compostos_schema() -> dict:
    """
    Garante estrutura para catálogo de compostos e vínculo no monitor.
    """
    global _MONITOR_COLUMNS_CACHE

    result = {
        "compostos_table_created": False,
        "compostos_desc_index_created": False,
        "monitor_composto_col_added": False,
        "monitor_composto_cod_col_added": False,
        "monitor_lote_col_added": False,
        "monitor_observacoes_col_added": False,
        "monitor_composto_cod_index_created": False,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, MONITOR_OWNER, COMPOSTOS_TABLE):
                cur.execute(
                    f"""
                    CREATE TABLE {COMPOSTOS_TABLE_FQN} (
                      CODPROD        NUMBER PRIMARY KEY,
                      DESCRICAO      VARCHAR2(200) NOT NULL,
                      CODGRUPOPROD   NUMBER NOT NULL,
                      ATIVO          CHAR(1) DEFAULT 'S' NOT NULL,
                      IMPORTADO_EM   TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
                    )
                    """
                )
                result["compostos_table_created"] = True

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{COMPOSTOS_INDEX_DESC} "
                    f"ON {COMPOSTOS_TABLE_FQN} (DESCRICAO)"
                )
                result["compostos_desc_index_created"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 955:
                    raise

            try:
                cur.execute(f"ALTER TABLE {MONITOR_TABLE_FQN} ADD (COMPOSTO VARCHAR2(200))")
                result["monitor_composto_col_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 1430:
                    raise

            try:
                cur.execute(f"ALTER TABLE {MONITOR_TABLE_FQN} ADD (COMPOSTO_COD NUMBER)")
                result["monitor_composto_cod_col_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 1430:
                    raise

            try:
                cur.execute(f"ALTER TABLE {MONITOR_TABLE_FQN} ADD (LOTE VARCHAR2(120))")
                result["monitor_lote_col_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 1430:
                    raise

            try:
                cur.execute(f"ALTER TABLE {MONITOR_TABLE_FQN} ADD (OBSERVACOES VARCHAR2(500))")
                result["monitor_observacoes_col_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 1430:
                    raise

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{COMPOSTO_COD_INDEX_NAME} "
                    f"ON {MONITOR_TABLE_FQN} (COMPOSTO_COD)"
                )
                result["monitor_composto_cod_index_created"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 955:
                    raise

        conn.commit()

    _MONITOR_COLUMNS_CACHE = None
    return result


def ensure_batch_id_schema() -> dict:
    """
    Garante coluna BATCH_ID e índice correspondente para rastreio de ciclos.
    """
    global _MONITOR_COLUMNS_CACHE

    result = {"batch_column_added": False, "batch_index_added": False}

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"ALTER TABLE {MONITOR_TABLE_FQN} ADD (BATCH_ID NUMBER)")
                result["batch_column_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                # ORA-01430: coluna já existe
                if err.code != 1430:
                    raise

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{BATCH_INDEX_NAME} "
                    f"ON {MONITOR_TABLE_FQN} (BATCH_ID)"
                )
                result["batch_index_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                # ORA-00955: nome já existe
                if err.code != 955:
                    raise

        conn.commit()

    _MONITOR_COLUMNS_CACHE = None
    return result


def ensure_batch_sequence(start_with: int = 1) -> dict:
    result = {"batch_sequence_created": False}
    with get_conn() as conn:
        with conn.cursor() as cur:
            if not _sequence_exists(cur, MONITOR_OWNER, BATCH_ID_SEQUENCE):
                cur.execute(
                    f"""
                    CREATE SEQUENCE {BATCH_ID_SEQUENCE_FQN}
                    START WITH {int(start_with)}
                    INCREMENT BY 1
                    NOCACHE
                    NOCYCLE
                    """
                )
                result["batch_sequence_created"] = True
        conn.commit()
    return result


def ensure_batch_tables_schema() -> dict:
    """
    Garante a tabela de batches e o vínculo FK do monitor com BATCH_ID.
    """
    ensure_batch_id_schema()

    result = {
        "batches_table_created": False,
        "monitor_batch_fk_added": False,
        "batch_start_index_added": False,
        "batch_end_index_added": False,
        "batch_op_index_added": False,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, MONITOR_OWNER, BATCHES_TABLE):
                cur.execute(
                    f"""
                    CREATE TABLE {BATCHES_TABLE_FQN} (
                      BATCH_ID       NUMBER PRIMARY KEY,
                      START_TS       TIMESTAMP NOT NULL,
                      END_TS         TIMESTAMP NULL,
                      DURATION_S     NUMBER NULL,
                      MAX_TEMP       NUMBER NULL,
                      MAX_CORRENTE   NUMBER NULL,
                      AVG_TEMP       NUMBER NULL,
                      AVG_CORRENTE   NUMBER NULL,
                      SIGNATURE      VARCHAR2(64) NULL,
                      OP             VARCHAR2(50) NULL,
                      OPERADOR       VARCHAR2(80) NULL,
                      OBSERVACOES    VARCHAR2(4000) NULL,
                      CREATED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                    )
                    """
                )
                result["batches_table_created"] = True

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{BATCHES_START_TS_INDEX_NAME} "
                    f"ON {BATCHES_TABLE_FQN} (START_TS)"
                )
                result["batch_start_index_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 955:
                    raise

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{BATCHES_END_TS_INDEX_NAME} "
                    f"ON {BATCHES_TABLE_FQN} (END_TS)"
                )
                result["batch_end_index_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 955:
                    raise

            try:
                cur.execute(
                    f"CREATE INDEX {MONITOR_OWNER}.{BATCHES_OP_INDEX_NAME} "
                    f"ON {BATCHES_TABLE_FQN} (OP)"
                )
                result["batch_op_index_added"] = True
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 955:
                    raise

            if not _constraint_exists(cur, MONITOR_OWNER, MONITOR_BATCH_FK_NAME):
                try:
                    cur.execute(
                        f"""
                        ALTER TABLE {MONITOR_TABLE_FQN}
                        ADD CONSTRAINT {MONITOR_BATCH_FK_NAME}
                        FOREIGN KEY (BATCH_ID)
                        REFERENCES {BATCHES_TABLE_FQN} (BATCH_ID)
                        ENABLE NOVALIDATE
                        """
                    )
                    result["monitor_batch_fk_added"] = True
                except oracledb.DatabaseError as e:
                    err, = e.args
                    # ORA-02275: constraint referencial já existe no objeto.
                    if err.code != 2275:
                        raise

        conn.commit()

    return result


def ensure_app_state_schema() -> dict:
    result = {"app_state_table_created": False}
    with get_conn() as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, MONITOR_OWNER, APP_STATE_TABLE):
                cur.execute(
                    f"""
                    CREATE TABLE {APP_STATE_TABLE_FQN} (
                      KEY         VARCHAR2(64) PRIMARY KEY,
                      VALUE       CLOB,
                      UPDATED_AT  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                    )
                    """
                )
                result["app_state_table_created"] = True
        conn.commit()
    return result


def ensure_cycle_tracking_schema() -> dict:
    out = {}
    out["batch_id"] = ensure_batch_id_schema()
    out["batch_sequence"] = ensure_batch_sequence()
    out["batches"] = ensure_batch_tables_schema()
    out["app_state"] = ensure_app_state_schema()
    return out


def _build_monitor_insert_sql(cur) -> tuple[str, bool]:
    optional_cols = _detect_optional_filter_columns(cur)
    batch_col = optional_cols["batch_id"]
    base_cols = [
        "TEMP_RAW",
        "CORRENTE_RAW",
        "TEMPERATURA_C",
        "CORRENTE",
        "BOTAO_START",
        "TAMPA_DESCARGA",
        "SRC",
    ]
    bind_cols = [
        ":temp_raw",
        ":corrente_raw",
        ":temperatura_c",
        ":corrente",
        ":botao_start",
        ":tampa_descarga",
        ":src",
    ]
    has_batch = bool(batch_col)
    if has_batch:
        base_cols.append(batch_col)
        bind_cols.append(":batch_id")
    sql = (
        f"INSERT INTO {MONITOR_TABLE_FQN} "
        f"({', '.join(base_cols)}) "
        f"VALUES ({', '.join(bind_cols)})"
    )
    return sql, has_batch


def _normalize_monitor_bind(row: dict, has_batch: bool) -> dict:
    bind = {
        "temp_raw": row.get("temp_raw"),
        "corrente_raw": row.get("corrente_raw"),
        "temperatura_c": row.get("temperatura_c"),
        "corrente": row.get("corrente"),
        "botao_start": row.get("botao_start"),
        "tampa_descarga": row.get("tampa_descarga"),
        "src": row.get("src"),
    }
    if has_batch:
        bind["batch_id"] = row.get("batch_id")
    return bind


def insert_readings_bulk(conn, rows: list[dict]) -> int:
    """
    Insercao em lote eficiente via executemany (1 commit por lote).
    """
    if not rows:
        return 0

    with conn.cursor() as cur:
        sql, has_batch = _build_monitor_insert_sql(cur)
        bind_rows = [_normalize_monitor_bind(row, has_batch=has_batch) for row in rows]
        cur.executemany(sql, bind_rows)

    conn.commit()
    return len(rows)


def insert_monitor_row(row: dict) -> dict:
    """
    Compatibilidade: mantém inserção unitária, delegando para o bulk.
    """
    with get_conn() as conn:
        insert_readings_bulk(conn, [row])
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ID, TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS')
                FROM {MONITOR_TABLE_FQN}
                ORDER BY ID DESC
                FETCH FIRST 1 ROWS ONLY
                """
            )
            latest = cur.fetchone()

    if not latest:
        return {"id": None, "ts": None}
    return {"id": _to_number(latest[0]), "ts": latest[1]}


def list_compostos_catalog(active_only: bool = True) -> list[dict]:
    ensure_compostos_schema()

    where = "WHERE ATIVO = 'S'" if active_only else ""
    sql = f"""
    SELECT CODPROD, DESCRICAO, CODGRUPOPROD, ATIVO, TO_CHAR(IMPORTADO_EM, 'YYYY-MM-DD HH24:MI:SS')
    FROM {COMPOSTOS_TABLE_FQN}
    {where}
    ORDER BY DESCRICAO
    """
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                rows.append(
                    {
                        "codigo": _to_int_if_numeric(r[0]),
                        "descricao": str(r[1]).strip() if r[1] is not None else None,
                        "grupo": _to_int_if_numeric(r[2]),
                        "ativo": r[3],
                        "importado_em": r[4],
                    }
                )
    return rows


def import_compostos_from_sankhya(grupo_alvo: int = 18011100) -> dict:
    ensure_compostos_schema()

    query = """
    SELECT
        PRO.CODPROD,
        PRO.DESCRPROD,
        PRO.CODGRUPOPROD
    FROM SANKHYA.TGFPRO PRO
    WHERE PRO.ATIVO = 'S'
      AND PRO.CODGRUPOPROD = :grupo
    """
    merge_sql = f"""
    MERGE INTO {COMPOSTOS_TABLE_FQN} dst
    USING (
        SELECT :codprod AS CODPROD, :descricao AS DESCRICAO, :grupo AS CODGRUPOPROD
        FROM dual
    ) src
    ON (dst.CODPROD = src.CODPROD)
    WHEN MATCHED THEN UPDATE SET
      dst.DESCRICAO = src.DESCRICAO,
      dst.CODGRUPOPROD = src.CODGRUPOPROD,
      dst.ATIVO = 'S',
      dst.IMPORTADO_EM = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT
      (CODPROD, DESCRICAO, CODGRUPOPROD, ATIVO, IMPORTADO_EM)
    VALUES
      (src.CODPROD, src.DESCRICAO, src.CODGRUPOPROD, 'S', SYSTIMESTAMP)
    """

    imported = 0
    items = []
    with get_conn() as conn:
        with conn.cursor() as cur_read, conn.cursor() as cur_write:
            cur_read.execute(query, grupo=int(grupo_alvo))
            source_rows = cur_read.fetchall()

            for row in source_rows:
                codigo = _to_int_if_numeric(row[0])
                descricao = str(row[1]).strip().upper() if row[1] is not None else ""
                grupo = _to_int_if_numeric(row[2]) or int(grupo_alvo)
                if codigo is None or not descricao:
                    continue

                cur_write.execute(
                    merge_sql,
                    codprod=int(codigo),
                    descricao=descricao,
                    grupo=int(grupo),
                )
                imported += 1
                items.append({"codigo": int(codigo), "descricao": descricao, "grupo": int(grupo)})

        conn.commit()

    return {
        "ok": True,
        "grupo": int(grupo_alvo),
        "importados": imported,
        "itens": items,
    }


def assign_composto_to_batch(
    batch_id: int,
    codprod: int | None = None,
    descricao: str | None = None,
    lote: str | None = None,
    observacoes: str | None = None,
) -> dict:
    ensure_compostos_schema()
    ensure_batch_id_schema()

    if codprod is None and not (descricao and descricao.strip()):
        return {
            "ok": False,
            "reason": "Informe codprod ou descricao para vincular.",
            "batch_id": int(batch_id),
            "updated_rows": 0,
        }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if codprod is not None:
                cur.execute(
                    f"""
                    SELECT CODPROD, DESCRICAO, CODGRUPOPROD
                    FROM {COMPOSTOS_TABLE_FQN}
                    WHERE CODPROD = :codprod
                    """,
                    codprod=int(codprod),
                )
            else:
                cur.execute(
                    f"""
                    SELECT CODPROD, DESCRICAO, CODGRUPOPROD
                    FROM {COMPOSTOS_TABLE_FQN}
                    WHERE UPPER(TRIM(DESCRICAO)) = :descricao
                    ORDER BY CODPROD
                    FETCH FIRST 1 ROWS ONLY
                    """,
                    descricao=descricao.strip().upper(),
                )
            comp = cur.fetchone()
            if not comp:
                return {
                    "ok": False,
                    "reason": (
                        f"Composto CODPROD={codprod} não encontrado no catálogo."
                        if codprod is not None
                        else f"Composto '{descricao}' não encontrado no catálogo."
                    ),
                    "batch_id": int(batch_id),
                    "updated_rows": 0,
                }

            optional_cols = _detect_optional_filter_columns(cur)
            batch_col = optional_cols["batch_id"]
            composto_col = optional_cols["composto"]
            lote_col = optional_cols["lote"]
            observacoes_col = optional_cols["observacoes"]

            if not batch_col:
                return {
                    "ok": False,
                    "reason": "Tabela de monitor sem coluna BATCH_ID.",
                    "batch_id": int(batch_id),
                    "updated_rows": 0,
                }

            if not composto_col:
                return {
                    "ok": False,
                    "reason": "Tabela de monitor sem coluna COMPOSTO.",
                    "batch_id": int(batch_id),
                    "updated_rows": 0,
                }

            lote_value = lote.strip().upper() if lote and lote.strip() else None
            observacoes_value = (
                observacoes.strip() if observacoes and observacoes.strip() else None
            )
            ignored = []

            set_parts = [f"{composto_col} = :descricao", "COMPOSTO_COD = :codprod"]
            binds = {
                "descricao": str(comp[1]).strip().upper(),
                "codprod": int(comp[0]),
                "batch_id": int(batch_id),
            }

            if lote_value:
                if lote_col:
                    set_parts.append(f"{lote_col} = :lote")
                    binds["lote"] = lote_value
                else:
                    ignored.append("LOTE não aplicado: coluna de lote não encontrada.")

            if observacoes_value:
                if observacoes_col:
                    set_parts.append(f"{observacoes_col} = :observacoes")
                    binds["observacoes"] = observacoes_value
                else:
                    ignored.append(
                        "OBSERVACOES não aplicado: coluna de observações não encontrada."
                    )

            cur.execute(
                f"""
                UPDATE {MONITOR_TABLE_FQN}
                SET {", ".join(set_parts)}
                WHERE {batch_col} = :batch_id
                """,
                binds,
            )
            updated_rows = int(cur.rowcount or 0)

        conn.commit()

    return {
        "ok": True,
        "batch_id": int(batch_id),
        "updated_rows": updated_rows,
        "composto": {
            "codigo": _to_int_if_numeric(comp[0]),
            "descricao": str(comp[1]).strip() if comp[1] is not None else None,
            "grupo": _to_int_if_numeric(comp[2]),
        },
        "lote": lote_value,
        "observacoes": observacoes_value,
        "ignored": ignored,
    }


def _to_number(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _to_int_if_numeric(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return value


def get_state(conn, key: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT VALUE
            FROM {APP_STATE_TABLE_FQN}
            WHERE KEY = :state_key
            """,
            state_key=key,
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None

    payload = row[0]
    if hasattr(payload, "read"):
        payload = payload.read()

    try:
        return json.loads(payload)
    except Exception as exc:
        logger.warning(f"Falha ao desserializar APP_STATE key={key}: {exc}")
        return None


def set_state(conn, key: str, dict_value: dict) -> None:
    payload = json.dumps(dict_value, ensure_ascii=True)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            MERGE INTO {APP_STATE_TABLE_FQN} dst
            USING (
                SELECT :state_key AS KEY, :state_value AS VALUE
                FROM dual
            ) src
            ON (dst.KEY = src.KEY)
            WHEN MATCHED THEN
              UPDATE SET dst.VALUE = src.VALUE, dst.UPDATED_AT = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
              INSERT (KEY, VALUE, UPDATED_AT)
              VALUES (src.KEY, src.VALUE, CURRENT_TIMESTAMP)
            """,
            state_key=key,
            state_value=payload,
        )


def next_batch_id_from_sequence(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {BATCH_ID_SEQUENCE}.NEXTVAL FROM dual")
        row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError(f"Falha ao obter NEXTVAL da sequence {BATCH_ID_SEQUENCE}.")
    return int(row[0])


def create_batch(
    conn,
    batch_id: int,
    start_ts: datetime,
    op: str | None = None,
    operador: str | None = None,
    observacoes: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {BATCHES_TABLE_FQN}
              (BATCH_ID, START_TS, OP, OPERADOR, OBSERVACOES)
            VALUES
              (:batch_id, :start_ts, :op, :operador, :observacoes)
            """,
            batch_id=int(batch_id),
            start_ts=start_ts,
            op=op,
            operador=operador,
            observacoes=observacoes,
        )
    return int(batch_id)


def close_batch(
    conn,
    batch_id: int,
    end_ts: datetime,
    duration_s: float | int | None,
    max_temp: float | None,
    max_corrente: float | None,
    avg_temp: float | None,
    avg_corrente: float | None,
    signature: str | None,
    observacoes: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {BATCHES_TABLE_FQN}
            SET END_TS = :end_ts,
                DURATION_S = :duration_s,
                MAX_TEMP = :max_temp,
                MAX_CORRENTE = :max_corrente,
                AVG_TEMP = :avg_temp,
                AVG_CORRENTE = :avg_corrente,
                SIGNATURE = :signature,
                OBSERVACOES = CASE
                    WHEN :observacoes IS NOT NULL THEN :observacoes
                    ELSE OBSERVACOES
                END
            WHERE BATCH_ID = :batch_id
            """,
            end_ts=end_ts,
            duration_s=duration_s,
            max_temp=max_temp,
            max_corrente=max_corrente,
            avg_temp=avg_temp,
            avg_corrente=avg_corrente,
            signature=signature,
            observacoes=observacoes,
            batch_id=int(batch_id),
        )
        return int(cur.rowcount or 0)


def close_batch_auto_after_downtime(
    batch_id: int,
    end_ts: datetime,
    observacoes: str = "auto-close after downtime",
) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {BATCHES_TABLE_FQN}
                SET END_TS = :end_ts,
                    DURATION_S = CASE
                        WHEN START_TS IS NOT NULL
                        THEN ROUND((CAST(:end_ts AS DATE) - CAST(START_TS AS DATE)) * 86400, 3)
                        ELSE DURATION_S
                    END,
                    OBSERVACOES = :observacoes
                WHERE BATCH_ID = :batch_id
                  AND END_TS IS NULL
                """,
                end_ts=end_ts,
                observacoes=observacoes,
                batch_id=int(batch_id),
            )
            updated = int(cur.rowcount or 0)
        conn.commit()
    return {"updated_rows": updated, "batch_id": int(batch_id)}


def fetch_batch_meta(batch_id: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  BATCH_ID,
                  START_TS,
                  END_TS,
                  DURATION_S,
                  MAX_TEMP,
                  MAX_CORRENTE,
                  AVG_TEMP,
                  AVG_CORRENTE,
                  SIGNATURE,
                  OP,
                  OPERADOR,
                  OBSERVACOES,
                  CREATED_AT
                FROM {BATCHES_TABLE_FQN}
                WHERE BATCH_ID = :batch_id
                """,
                batch_id=int(batch_id),
            )
            row = cur.fetchone()

    if not row:
        return None

    return {
        "batch_id": _to_int_if_numeric(row[0]),
        "start_ts": row[1],
        "end_ts": row[2],
        "duration_s": _to_number(row[3]),
        "max_temp": _to_number(row[4]),
        "max_corrente": _to_number(row[5]),
        "avg_temp": _to_number(row[6]),
        "avg_corrente": _to_number(row[7]),
        "signature": str(row[8]).strip() if row[8] is not None else None,
        "op": str(row[9]).strip() if row[9] is not None else None,
        "operador": str(row[10]).strip() if row[10] is not None else None,
        "observacoes": str(row[11]).strip() if row[11] is not None else None,
        "created_at": row[12],
    }


def fetch_batch_readings(batch_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            optional_cols = _detect_optional_filter_columns(cur)
            batch_col = optional_cols["batch_id"]
            if not batch_col:
                return []

            cur.execute(
                f"""
                SELECT
                  ID,
                  TS,
                  TEMPERATURA_C,
                  CORRENTE,
                  BOTAO_START,
                  TAMPA_DESCARGA,
                  TEMP_RAW,
                  CORRENTE_RAW
                FROM {MONITOR_TABLE_FQN}
                WHERE TRIM(TO_CHAR({batch_col})) = :batch_id
                ORDER BY TS ASC, ID ASC
                """,
                batch_id=str(int(batch_id)),
            )
            rows = cur.fetchall()

    out = []
    for row in rows:
        out.append(
            {
                "id": _to_number(row[0]),
                "ts": row[1],
                "temp": _to_number(row[2]),
                "corrente": _to_number(row[3]),
                "start_signal": _to_number(row[4]),
                "lid_signal": _to_number(row[5]),
                "temp_raw": _to_number(row[6]),
                "corr_raw": _to_number(row[7]),
            }
        )
    return out


def update_batch_signature_if_empty(batch_id: int, signature: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {BATCHES_TABLE_FQN}
                SET SIGNATURE = :signature
                WHERE BATCH_ID = :batch_id
                  AND (SIGNATURE IS NULL OR TRIM(SIGNATURE) = '')
                """,
                signature=signature,
                batch_id=int(batch_id),
            )
            updated = int(cur.rowcount or 0)
        conn.commit()
    return updated


def _build_period_where(
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> tuple[list[str], dict]:
    where_parts = []
    binds = {}
    if start_ts:
        where_parts.append("TS >= :start_ts")
        binds["start_ts"] = start_ts
    if end_ts:
        where_parts.append("TS <= :end_ts")
        binds["end_ts"] = end_ts
    return where_parts, binds


def _fetch_distinct_values(
    cur,
    column_name: str | None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> list[str]:
    if not column_name:
        return []

    where_parts, binds = _build_period_where(start_ts=start_ts, end_ts=end_ts)
    where_parts.append(f"{column_name} IS NOT NULL")
    sql = f"""
    SELECT DISTINCT TO_CHAR({column_name}) AS VAL
    FROM {MONITOR_TABLE_FQN}
    WHERE {" AND ".join(where_parts)}
    ORDER BY 1
    """
    cur.execute(sql, binds)

    values = []
    for r in cur.fetchall():
        if r[0] is None:
            continue
        v = str(r[0]).strip()
        if v:
            values.append(v)
    return values


def fetch_history_filter_options(
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            optional_cols = _detect_optional_filter_columns(cur)
            compostos_catalog = []
            try:
                cur.execute(
                    f"""
                    SELECT DESCRICAO
                    FROM {COMPOSTOS_TABLE_FQN}
                    WHERE ATIVO = 'S'
                    ORDER BY DESCRICAO
                    """
                )
                compostos_catalog = [str(r[0]).strip() for r in cur.fetchall() if r[0]]
            except oracledb.DatabaseError as e:
                err, = e.args
                if err.code != 942:
                    raise

            return {
                "columns": optional_cols,
                "options": {
                    "lote": _fetch_distinct_values(
                        cur,
                        optional_cols["lote"],
                        start_ts=start_ts,
                        end_ts=end_ts,
                    ),
                    "composto": (
                        compostos_catalog
                        if compostos_catalog
                        else _fetch_distinct_values(
                            cur,
                            optional_cols["composto"],
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )
                    ),
                    "batch_id": _fetch_distinct_values(
                        cur,
                        optional_cols["batch_id"],
                        start_ts=start_ts,
                        end_ts=end_ts,
                    ),
                },
            }


def fetch_monitor_history(
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    lote: str | None = None,
    composto: str | None = None,
    batch_id: str | None = None,
    op: str | None = None,
) -> dict:
    where_parts, binds = _build_period_where(start_ts=start_ts, end_ts=end_ts)

    filters_meta = {
        "period": {
            "start": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else None,
            "end": end_ts.strftime("%Y-%m-%d %H:%M:%S") if end_ts else None,
        },
        "lote": {"requested": lote, "column": None, "applied": False, "ignored_reason": None},
        "composto": {
            "requested": composto,
            "column": None,
            "applied": False,
            "ignored_reason": None,
        },
        "batch_id": {
            "requested": batch_id,
            "column": None,
            "applied": False,
            "ignored_reason": None,
        },
        "op": {"requested": op, "column": None, "applied": False, "ignored_reason": None},
    }

    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            optional_cols = _detect_optional_filter_columns(cur)
            lote_col = optional_cols["lote"]
            composto_col = optional_cols["composto"]
            batch_col = optional_cols["batch_id"]
            observacoes_col = optional_cols["observacoes"]
            lote_select = (
                f"TO_CHAR({lote_col}) AS LOTE"
                if lote_col
                else "CAST(NULL AS VARCHAR2(120)) AS LOTE"
            )
            composto_select = (
                f"TO_CHAR({composto_col}) AS COMPOSTO"
                if composto_col
                else "CAST(NULL AS VARCHAR2(200)) AS COMPOSTO"
            )
            batch_select = (
                f"TO_CHAR({batch_col}) AS BATCH_ID"
                if batch_col
                else "CAST(NULL AS VARCHAR2(50)) AS BATCH_ID"
            )
            observacoes_select = (
                f"TO_CHAR({observacoes_col}) AS OBSERVACOES"
                if observacoes_col
                else "CAST(NULL AS VARCHAR2(500)) AS OBSERVACOES"
            )

            select_sql = f"""
            SELECT
              ID,
              TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') AS TS,
              TEMPERATURA_C AS TEMPERATURA,
              CORRENTE,
              TAMPA_DESCARGA AS TAMPA,
              BOTAO_START AS START_FLAG,
              TEMP_RAW,
              CORRENTE_RAW AS CORR_RAW,
              {composto_select},
              {batch_select},
              {lote_select},
              {observacoes_select}
            FROM {MONITOR_TABLE_FQN}
            """

            if lote and lote.strip():
                filters_meta["lote"]["column"] = lote_col
                if lote_col:
                    where_parts.append(f"UPPER(TRIM(TO_CHAR({lote_col}))) = :lote")
                    binds["lote"] = lote.strip().upper()
                    filters_meta["lote"]["applied"] = True
                else:
                    filters_meta["lote"]["ignored_reason"] = (
                        "Tabela sem coluna de lote (ex.: LOTE)."
                    )

            if composto and composto.strip():
                filters_meta["composto"]["column"] = composto_col
                if composto_col:
                    where_parts.append(f"UPPER(TRIM(TO_CHAR({composto_col}))) = :composto")
                    binds["composto"] = composto.strip().upper()
                    filters_meta["composto"]["applied"] = True
                else:
                    filters_meta["composto"]["ignored_reason"] = (
                        "Tabela sem coluna de composto (ex.: COMPOSTO/RECEITA)."
                    )

            if batch_id and batch_id.strip():
                filters_meta["batch_id"]["column"] = batch_col
                if batch_col:
                    where_parts.append(f"UPPER(TRIM(TO_CHAR({batch_col}))) = :batch_id")
                    binds["batch_id"] = batch_id.strip().upper()
                    filters_meta["batch_id"]["applied"] = True
                else:
                    filters_meta["batch_id"]["ignored_reason"] = (
                        "Tabela sem coluna de batch (ex.: BATCH_ID)."
                    )

            op_col = optional_cols["op"]
            if op and op.strip():
                filters_meta["op"]["column"] = op_col
                if op_col:
                    where_parts.append(f"UPPER(TRIM(TO_CHAR({op_col}))) = :op")
                    binds["op"] = op.strip().upper()
                    filters_meta["op"]["applied"] = True
                else:
                    filters_meta["op"]["ignored_reason"] = (
                        "Tabela sem coluna de OP (ex.: OP/ORDEM_PRODUCAO)."
                    )

            sql = select_sql
            if where_parts:
                sql += "\nWHERE " + "\n  AND ".join(where_parts)
            sql += "\nORDER BY TS DESC"

            cur.execute(sql, binds)
            for r in cur.fetchall():
                rows.append(
                    {
                        "id": _to_number(r[0]),
                        "ts": r[1],
                        "temperatura": _to_number(r[2]),
                        "corrente": _to_number(r[3]),
                        "tampa": _to_number(r[4]),
                        "start": _to_number(r[5]),
                        "temp_raw": _to_number(r[6]),
                        "corr_raw": _to_number(r[7]),
                        "composto": r[8],
                        "batch_id": _to_int_if_numeric(r[9]),
                        "lote": r[10],
                        "observacoes": r[11],
                    }
                )

    return {"rows": rows, "filters": filters_meta}


def _as_bit(value) -> int:
    try:
        return 1 if int(value) > 0 else 0
    except Exception:
        return 0


def backfill_batch_ids(fetch_batch_size: int = 2000, commit: bool = True) -> dict:
    """
    Recalcula BATCH_ID de todas as leituras usando regra de borda:
      - start 0->1 inicia batch
      - tampa 1->0 encerra batch (1=fechada, 0=aberta)
    """
    ensure_batch_id_schema()

    total_rows = 0
    updated_rows = 0

    next_batch_id = 1
    current_batch_id: int | None = None
    cycle_active = False
    prev_start: int | None = None
    prev_tampa_descarga: int | None = None

    with get_conn() as conn:
        with conn.cursor() as cur_read, conn.cursor() as cur_write:
            optional_cols = _detect_optional_filter_columns(cur_read)
            batch_col = optional_cols["batch_id"]
            if not batch_col:
                return {
                    "ok": False,
                    "reason": "Coluna de batch não encontrada.",
                    "total_rows": 0,
                    "updated_rows": 0,
                }

            cur_read.execute(
                f"""
                SELECT ID, BOTAO_START, TAMPA_DESCARGA, {batch_col}
                FROM {MONITOR_TABLE_FQN}
                ORDER BY TS ASC, ID ASC
                """
            )

            update_sql = (
                f"UPDATE {MONITOR_TABLE_FQN} "
                f"SET {batch_col} = :batch_id "
                f"WHERE ID = :id"
            )
            pending_updates = []

            while True:
                chunk = cur_read.fetchmany(fetch_batch_size)
                if not chunk:
                    break

                for row in chunk:
                    row_id, start_raw, tampa_raw, existing_batch_raw = row
                    total_rows += 1

                    start = _as_bit(start_raw)
                    tampa = _as_bit(tampa_raw)

                    start_rising = prev_start == 0 and start == 1
                    tampa_open_edge = prev_tampa_descarga == 1 and tampa == 0

                    if not cycle_active:
                        if prev_start is None and prev_tampa_descarga is None:
                            if start == 1 and tampa == 1:
                                current_batch_id = next_batch_id
                                next_batch_id += 1
                                cycle_active = True
                        elif start_rising:
                            current_batch_id = next_batch_id
                            next_batch_id += 1
                            cycle_active = True

                    computed_batch_id = current_batch_id if cycle_active else None

                    if cycle_active and tampa_open_edge:
                        cycle_active = False
                        current_batch_id = None

                    prev_start = start
                    prev_tampa_descarga = tampa

                    existing_batch_id = _to_int_if_numeric(existing_batch_raw)
                    if existing_batch_id != computed_batch_id:
                        pending_updates.append(
                            {"id": int(row_id), "batch_id": computed_batch_id}
                        )
                        updated_rows += 1

                if pending_updates:
                    cur_write.executemany(update_sql, pending_updates)
                    pending_updates.clear()

        if commit:
            conn.commit()
        else:
            conn.rollback()

    return {
        "ok": True,
        "total_rows": total_rows,
        "updated_rows": updated_rows,
        "committed": commit,
        "next_batch_id_preview": next_batch_id,
    }
