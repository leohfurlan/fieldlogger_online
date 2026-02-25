# app/db.py
from datetime import datetime
from decimal import Decimal

import oracledb

from config import get_conn

MONITOR_OWNER = "ENGENHARIA"
MONITOR_TABLE = "FIELDLOGGER_MONITOR"
MONITOR_TABLE_FQN = f"{MONITOR_OWNER}.{MONITOR_TABLE}"
BATCH_INDEX_NAME = "IX_FL_MONITOR_BATCH_ID"
COMPOSTO_COD_INDEX_NAME = "IX_FL_MONITOR_COMP_COD"
COMPOSTOS_TABLE = "FIELDLOGGER_COMPOSTOS"
COMPOSTOS_TABLE_FQN = f"{MONITOR_OWNER}.{COMPOSTOS_TABLE}"
COMPOSTOS_INDEX_DESC = "IX_FL_COMPOSTOS_DESC"

LOTE_COLUMN_CANDIDATES = ("LOTE", "LOTE_COMPOSTO", "LOTE_BANBURY")
COMPOSTO_COLUMN_CANDIDATES = ("COMPOSTO", "COMPOSTO_ID", "COD_COMPOSTO", "RECEITA")
BATCH_ID_COLUMN_CANDIDATES = ("BATCH_ID", "BATCH", "ID_BATCH", "BATELADA")
OP_COLUMN_CANDIDATES = ("OP", "ORDEM_PRODUCAO", "ORDEM_PROD", "ORDEM")
OBSERVACOES_COLUMN_CANDIDATES = ("OBSERVACOES", "OBSERVACAO", "OBS", "ANOTACAO")

_MONITOR_COLUMNS_CACHE: set[str] | None = None


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


def insert_monitor_row(row: dict) -> dict:
    """
    Insere e retorna {"id": <ID>, "ts": <timestamp string>}.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            optional_cols = _detect_optional_filter_columns(cur)
            batch_col = optional_cols["batch_id"]

            out_id = cur.var(int)
            out_ts = cur.var(str)

            binds = {
                "temp_raw": row.get("temp_raw"),
                "corrente_raw": row.get("corrente_raw"),
                "temperatura_c": row.get("temperatura_c"),
                "corrente": row.get("corrente"),
                "botao_start": row.get("botao_start"),
                "tampa_descarga": row.get("tampa_descarga"),
                "src": row.get("src"),
                "out_id": out_id,
                "out_ts": out_ts,
            }

            if batch_col:
                sql = f"""
                INSERT INTO {MONITOR_TABLE_FQN}
                  (TEMP_RAW, CORRENTE_RAW, TEMPERATURA_C, CORRENTE, BOTAO_START, TAMPA_DESCARGA, SRC, {batch_col})
                VALUES
                  (:temp_raw, :corrente_raw, :temperatura_c, :corrente, :botao_start, :tampa_descarga, :src, :batch_id)
                RETURNING ID, TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') INTO :out_id, :out_ts
                """
                binds["batch_id"] = row.get("batch_id")
            else:
                sql = f"""
                INSERT INTO {MONITOR_TABLE_FQN}
                  (TEMP_RAW, CORRENTE_RAW, TEMPERATURA_C, CORRENTE, BOTAO_START, TAMPA_DESCARGA, SRC)
                VALUES
                  (:temp_raw, :corrente_raw, :temperatura_c, :corrente, :botao_start, :tampa_descarga, :src)
                RETURNING ID, TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') INTO :out_id, :out_ts
                """

            cur.execute(sql, binds)

        conn.commit()

    return {"id": out_id.getvalue(), "ts": out_ts.getvalue()}


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


def fetch_batch_runtime_state() -> dict:
    """
    Lê o estado recente para continuar sequência de batch após restart da aplicação.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            optional_cols = _detect_optional_filter_columns(cur)
            batch_col = optional_cols["batch_id"]

            if not batch_col:
                return {
                    "batch_column": None,
                    "next_batch_id": 1,
                    "last_start": None,
                    "last_tampa_descarga": None,
                    "last_batch_id": None,
                }

            cur.execute(
                f"""
                SELECT NVL(
                    MAX(
                        CASE
                            WHEN REGEXP_LIKE(TRIM(TO_CHAR({batch_col})), '^[0-9]+$')
                            THEN TO_NUMBER(TRIM(TO_CHAR({batch_col})))
                        END
                    ),
                    0
                )
                FROM {MONITOR_TABLE_FQN}
                """
            )
            max_batch_value = _to_int_if_numeric(cur.fetchone()[0])
            max_batch_id = int(max_batch_value or 0)

            cur.execute(
                f"""
                SELECT BOTAO_START, TAMPA_DESCARGA, TO_CHAR({batch_col}) AS BATCH_ID
                FROM {MONITOR_TABLE_FQN}
                ORDER BY TS DESC
                FETCH FIRST 1 ROWS ONLY
                """
            )
            latest = cur.fetchone()

            if latest:
                last_start = _to_int_if_numeric(latest[0])
                last_tampa = _to_int_if_numeric(latest[1])
                last_batch_id = _to_int_if_numeric(latest[2])
            else:
                last_start = None
                last_tampa = None
                last_batch_id = None

            return {
                "batch_column": batch_col,
                "next_batch_id": max_batch_id + 1,
                "last_start": last_start,
                "last_tampa_descarga": last_tampa,
                "last_batch_id": last_batch_id,
            }


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
