import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import os
from pathlib import Path
from typing import Set

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

# Imports que funcionam tanto rodando "uvicorn main:app" dentro da pasta app
# quanto rodando "uvicorn app.main:app" a partir da pasta pai.
try:
    import db as db_layer
    from config import logger
    from cycle_detector import CycleLifecycleManager
    from db import (
        assign_composto_to_batch,
        close_pool,
        ensure_compostos_schema,
        ensure_cycle_tracking_schema,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        list_compostos_catalog,
        list_tables,
    )
    from modbus_client import FieldLoggerModbus
    from readings_buffer import ReadingsBuffer
except ModuleNotFoundError:
    from . import db as db_layer
    from .config import logger
    from .cycle_detector import CycleLifecycleManager
    from .db import (
        assign_composto_to_batch,
        close_pool,
        ensure_compostos_schema,
        ensure_cycle_tracking_schema,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        list_compostos_catalog,
        list_tables,
    )
    from .modbus_client import FieldLoggerModbus
    from .readings_buffer import ReadingsBuffer


BASE_DIR = Path(__file__).resolve().parent


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip()
    if not text:
        return default
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        text = text[1:-1].strip()
    return text or default


def _env_int(name: str, default: int) -> int:
    text = _env_str(name, str(default))
    try:
        return int(text)
    except ValueError as exc:
        raise RuntimeError(
            f"Variavel de ambiente invalida: {name}='{text}'. Informe numero inteiro."
        ) from exc


def _env_float(name: str, default: float) -> float:
    text = _env_str(name, str(default))
    try:
        return float(text)
    except ValueError as exc:
        raise RuntimeError(
            f"Variavel de ambiente invalida: {name}='{text}'. Informe numero."
        ) from exc


# -------- MODBUS CONFIG (via .env) --------
MODBUS_HOST = _env_str("MODBUS_HOST", "172.16.30.95")
MODBUS_PORT = _env_int("MODBUS_PORT", 502)
MODBUS_UNIT_ID = _env_int("MODBUS_UNIT_ID", 255)
POLL_SECONDS = _env_float("POLL_SECONDS", 1.0)
READINGS_BATCH_SIZE = _env_int("READINGS_BATCH_SIZE", 100)
READINGS_FLUSH_INTERVAL_S = _env_float("READINGS_FLUSH_INTERVAL_S", 2.0)
CYCLE_STATE_PERSIST_INTERVAL_S = _env_float("CYCLE_STATE_PERSIST_INTERVAL_S", 10.0)
CYCLE_STALE_TIMEOUT_S = _env_float("CYCLE_STALE_TIMEOUT_S", 2 * 60 * 60)
CYCLE_SIGNATURE_SAMPLE_EVERY = _env_int("CYCLE_SIGNATURE_SAMPLE_EVERY", 10)

# -------- WS CLIENTS --------
ws_clients: Set[WebSocket] = set()

readings_buffer = ReadingsBuffer(
    max_batch_size=READINGS_BATCH_SIZE,
    flush_interval_s=READINGS_FLUSH_INTERVAL_S,
)
cycle_detector = CycleLifecycleManager(
    db_layer=db_layer,
    state_key=db_layer.STATE_KEY_CYCLE_DETECTOR,
    state_persist_interval_s=CYCLE_STATE_PERSIST_INTERVAL_S,
    stale_cycle_timeout_s=CYCLE_STALE_TIMEOUT_S,
    signature_sample_every=CYCLE_SIGNATURE_SAMPLE_EVERY,
)


async def broadcaster(payload: dict) -> None:
    dead = []
    # Snapshot avoids "Set changed size during iteration" when clients
    # connect/disconnect while we are broadcasting.
    for ws in list(ws_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.discard(ws)


async def poller() -> None:
    """
    Loop principal:
      - conecta Modbus TCP
      - le registradores
      - grava em lote no Oracle
      - envia ao vivo via WebSocket
    Tem reconexao automatica se cair rede/modbus.
    """
    while True:
        fl = FieldLoggerModbus(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT_ID)
        try:
            if not await asyncio.to_thread(fl.connect):
                raise RuntimeError("Falha ao conectar no Modbus TCP (connect=False).")

            while True:
                data = await asyncio.to_thread(fl.read)

                try:
                    decision = cycle_detector.process_reading(data)
                    await asyncio.to_thread(cycle_detector.sync_db, readings_buffer)

                    resolved_batch_id = decision.batch_id
                    if decision.cycle_token:
                        recovered_batch_id = cycle_detector.batch_id_for_token(
                            decision.cycle_token
                        )
                        if recovered_batch_id is not None:
                            resolved_batch_id = recovered_batch_id

                    readings_buffer.append(
                        {
                            "temp_raw": data.get("temperatura_term_raw"),
                            "corrente_raw": data.get("corrente_raw"),
                            "temperatura_c": data.get("temperatura_term"),
                            "corrente": data.get("corrente"),
                            "botao_start": data.get("botao_start"),
                            "tampa_descarga": data.get("tampa_descarga"),
                            "batch_id": resolved_batch_id,
                            "src": f"{MODBUS_HOST}:{MODBUS_PORT}/u{MODBUS_UNIT_ID}",
                            "_cycle_token": decision.cycle_token,
                        }
                    )

                    if decision.cycle_token and resolved_batch_id is not None:
                        readings_buffer.assign_batch_id(
                            decision.cycle_token,
                            resolved_batch_id,
                        )

                    if readings_buffer.should_flush():
                        await asyncio.to_thread(readings_buffer.flush)

                    if decision.start_event or decision.end_event:
                        await asyncio.to_thread(cycle_detector.persist_state, True)
                    else:
                        await asyncio.to_thread(cycle_detector.persist_state, False)

                    data["batch_id"] = resolved_batch_id
                    if readings_buffer.failure_count > 0:
                        data["_db_failures"] = readings_buffer.failure_count
                except Exception as exc:
                    # Nao derruba tempo real por falha de banco
                    data["_db_error"] = str(exc)

                await broadcaster(data)
                await asyncio.sleep(POLL_SECONDS)

        except asyncio.CancelledError:
            # shutdown limpo
            try:
                await asyncio.to_thread(fl.close)
            except Exception:
                pass
            raise
        except Exception as exc:
            # caiu modbus/rede: espera e tenta reconectar
            try:
                await asyncio.to_thread(fl.close)
            except Exception:
                pass
            await broadcaster({"_status": "modbus_down", "_error": str(exc)})
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.to_thread(ensure_compostos_schema)
    except Exception as exc:
        logger.warning("Falha ao garantir schema de compostos: {}", exc)

    try:
        await asyncio.to_thread(ensure_cycle_tracking_schema)
    except Exception as exc:
        logger.warning("Falha ao garantir schema de ciclo: {}", exc)

    await asyncio.to_thread(cycle_detector.startup)

    task = asyncio.create_task(poller())
    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        await asyncio.to_thread(cycle_detector.sync_db, readings_buffer)
        await asyncio.to_thread(readings_buffer.flush, True)
        await asyncio.to_thread(cycle_detector.shutdown)
        await asyncio.to_thread(close_pool)


app = FastAPI(lifespan=lifespan)


def _parse_history_datetime(value: str | None, field_name: str) -> datetime | None:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    # Aceita ISO e tambem formato Oracle retornado pela aplicacao.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d" and field_name == "end":
                return dt.replace(hour=23, minute=59, second=59)
            return dt
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Parametro invalido: '{field_name}'. Use 'YYYY-MM-DD', "
                "'YYYY-MM-DDTHH:MM' ou 'YYYY-MM-DD HH:MM:SS'."
            ),
        ) from exc


@app.get("/", response_class=HTMLResponse)
def index():
    html_path = BASE_DIR / "templates" / "index.html"
    if not html_path.exists():
        return HTMLResponse(
            "<h3>index.html nao encontrado em templates/</h3>", status_code=500
        )
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/historico", response_class=HTMLResponse)
def history_page():
    html_path = BASE_DIR / "templates" / "history.html"
    if not html_path.exists():
        return HTMLResponse(
            "<h3>history.html nao encontrado em templates/</h3>", status_code=500
        )
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/history")
def history_data(
    start: str | None = Query(
        default=None,
        description="Inicio do periodo. Ex.: 2026-02-25T07:30 ou 2026-02-25 07:30:00",
    ),
    end: str | None = Query(
        default=None,
        description="Fim do periodo. Ex.: 2026-02-25T08:00 ou 2026-02-25 08:00:00",
    ),
    lote: str | None = Query(default=None, description="Filtro opcional por lote"),
    composto: str | None = Query(default=None, description="Filtro opcional por composto"),
    batch_id: str | None = Query(default=None, description="Filtro opcional por batch"),
    op: str | None = Query(default=None, description="Filtro opcional por OP"),
):
    start_ts = _parse_history_datetime(start, "start")
    end_ts = _parse_history_datetime(end, "end")
    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(
            status_code=422,
            detail="Parametro invalido: 'start' deve ser menor ou igual a 'end'.",
        )
    return fetch_monitor_history(
        start_ts=start_ts,
        end_ts=end_ts,
        lote=lote,
        composto=composto,
        batch_id=batch_id,
        op=op,
    )


@app.get("/api/history/options")
def history_options(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
):
    start_ts = _parse_history_datetime(start, "start")
    end_ts = _parse_history_datetime(end, "end")
    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(
            status_code=422,
            detail="Parametro invalido: 'start' deve ser menor ou igual a 'end'.",
        )
    return fetch_history_filter_options(start_ts=start_ts, end_ts=end_ts)


@app.get("/api/compostos")
def compostos_list(active_only: bool = Query(default=True)):
    return {"items": list_compostos_catalog(active_only=active_only)}


@app.post("/api/compostos/import")
def compostos_import(grupo: int = Query(default=18011100)):
    return import_compostos_from_sankhya(grupo_alvo=grupo)


@app.post("/api/batches/{batch_id}/composto")
def batch_assign_composto(
    batch_id: int,
    codprod: int | None = Query(default=None),
    descricao: str | None = Query(default=None),
    lote: str | None = Query(default=None),
    observacoes: str | None = Query(default=None),
):
    result = assign_composto_to_batch(
        batch_id=batch_id,
        codprod=codprod,
        descricao=descricao,
        lote=lote,
        observacoes=observacoes,
    )
    if not result.get("ok"):
        raise HTTPException(
            status_code=400,
            detail=result.get("reason", "Falha ao vincular composto."),
        )
    return result


@app.get("/health/db")
def health_db():
    # Se conectar e listar, ta ok
    tables = list_tables("ENGENHARIA")
    return {"ok": True, "owner": "ENGENHARIA", "tables_count": len(tables)}


@app.get("/db/tables")
def db_tables():
    return {"owner": "ENGENHARIA", "tables": list_tables("ENGENHARIA")}


@app.get("/health/modbus")
def health_modbus():
    # check rapido (sem ficar preso)
    fl = FieldLoggerModbus(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT_ID)
    ok = fl.connect()
    try:
        if not ok:
            return JSONResponse({"ok": False}, status_code=503)
        data = fl.read()
        return {"ok": True, "sample": data}
    finally:
        try:
            fl.close()
        except Exception:
            pass


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        while True:
            # Mantem a conexao aberta; dados chegam via broadcaster()
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)
