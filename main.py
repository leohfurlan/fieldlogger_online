import asyncio
import io
from contextlib import asynccontextmanager
from datetime import datetime
import os
from pathlib import Path
import threading
import time
from typing import Literal, Set

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

load_dotenv()

# Imports que funcionam tanto rodando "uvicorn main:app" dentro da pasta app
# quanto rodando "uvicorn app.main:app" a partir da pasta pai.
if __package__:
    from . import db as db_layer
    from .batch_analysis import (
        build_delta_profile,
        build_signature_payload,
        compute_batch_metrics,
        compute_compare_metrics,
        derive_batch_events,
    )
    from .batch_reporting import (
        build_batch_summary_excel,
        build_excel_report,
        build_pdf_report,
    )
    from .config import logger
    from .cycle_detector import CycleLifecycleManager
    from .db import (
        assign_composto_to_batch,
        close_pool,
        ensure_compostos_schema,
        ensure_cycle_tracking_schema,
        fetch_batch_meta,
        fetch_batch_readings,
        fetch_history_batch_index,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        list_compostos_catalog,
        list_tables,
        update_batch_signature_if_empty,
    )
    from .modbus_client import FieldLoggerModbus
    from .readings_buffer import ReadingsBuffer
else:
    import db as db_layer
    from batch_analysis import (
        build_delta_profile,
        build_signature_payload,
        compute_batch_metrics,
        compute_compare_metrics,
        derive_batch_events,
    )
    from batch_reporting import build_batch_summary_excel, build_excel_report, build_pdf_report
    from config import logger
    from cycle_detector import CycleLifecycleManager
    from db import (
        assign_composto_to_batch,
        close_pool,
        ensure_compostos_schema,
        ensure_cycle_tracking_schema,
        fetch_batch_meta,
        fetch_batch_readings,
        fetch_history_batch_index,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        list_compostos_catalog,
        list_tables,
        update_batch_signature_if_empty,
    )
    from modbus_client import FieldLoggerModbus
    from readings_buffer import ReadingsBuffer


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
MODBUS_TIMEOUT_S = _env_float("MODBUS_TIMEOUT_S", 5.0)
POLL_SECONDS = _env_float("POLL_SECONDS", 1.0)
READINGS_BATCH_SIZE = _env_int("READINGS_BATCH_SIZE", 100)
READINGS_FLUSH_INTERVAL_S = _env_float("READINGS_FLUSH_INTERVAL_S", 3.0)
READINGS_QUEUE_LIMIT = _env_int("READINGS_QUEUE_LIMIT", 5000)
CYCLE_STATE_PERSIST_INTERVAL_S = _env_float("CYCLE_STATE_PERSIST_INTERVAL_S", 10.0)
CYCLE_STALE_TIMEOUT_S = _env_float("CYCLE_STALE_TIMEOUT_S", 2 * 60 * 60)
CYCLE_SIGNATURE_SAMPLE_EVERY = _env_int("CYCLE_SIGNATURE_SAMPLE_EVERY", 10)
DB_SYNC_INTERVAL_S = _env_float("DB_SYNC_INTERVAL_S", 1.0)
DB_TICK_INTERVAL_S = _env_float("DB_TICK_INTERVAL_S", 0.5)
BATCH_METRICS_CACHE_TTL_S = max(1.0, _env_float("BATCH_METRICS_CACHE_TTL_S", 60.0))
BATCH_SUMMARY_CACHE_TTL_S = max(1.0, _env_float("BATCH_SUMMARY_CACHE_TTL_S", 60.0))

# -------- WS CLIENTS --------
ws_clients: Set[WebSocket] = set()
_batch_payload_cache_lock = threading.Lock()
_batch_payload_cache: dict[int, tuple[float, tuple[dict, list[dict], list[dict], dict]]] = {}
_batch_summary_cache: dict[str, tuple[float, dict]] = {}

readings_buffer = ReadingsBuffer(
    max_batch_size=READINGS_BATCH_SIZE,
    flush_interval_s=READINGS_FLUSH_INTERVAL_S,
    max_queue_size=READINGS_QUEUE_LIMIT,
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
    last_db_tick_monotonic = 0.0
    last_sync_monotonic = 0.0

    while True:
        fl = FieldLoggerModbus(
            MODBUS_HOST,
            MODBUS_PORT,
            MODBUS_UNIT_ID,
            timeout=MODBUS_TIMEOUT_S,
        )
        try:
            if not await asyncio.to_thread(fl.connect):
                raise RuntimeError("Falha ao conectar no Modbus TCP (connect=False).")
            logger.bind(component="poller", event="modbus_connected").info(
                "Conexao Modbus estabelecida host={} port={} unit_id={}",
                MODBUS_HOST,
                MODBUS_PORT,
                MODBUS_UNIT_ID,
            )
            last_db_tick_monotonic = 0.0
            last_sync_monotonic = 0.0

            while True:
                loop_started_monotonic = time.monotonic()
                data = await asyncio.to_thread(fl.read)
                data["_modbus_status"] = "OK"
                start_event = False
                end_event = False

                try:
                    decision = cycle_detector.process_reading(data)
                    start_event = bool(decision.start_event)
                    end_event = bool(decision.end_event)

                    # Eventos de borda tentam sincronizar imediatamente para resolver batch_id.
                    if start_event or end_event:
                        await asyncio.to_thread(cycle_detector.sync_db, readings_buffer)
                        last_sync_monotonic = time.monotonic()

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

                    data["batch_id"] = resolved_batch_id
                    if resolved_batch_id is not None:
                        # Nova leitura invalida cache de metricas do batch em andamento.
                        _invalidate_runtime_caches(batch_id=resolved_batch_id)
                    elif start_event or end_event:
                        _invalidate_runtime_caches()

                    if readings_buffer.failure_count > 0:
                        data["_db_failures"] = readings_buffer.failure_count
                except Exception as exc:
                    # Nao derruba tempo real por falha de banco
                    data["_db_error"] = str(exc)
                data["_db_status"] = readings_buffer.db_status

                await broadcaster(data)

                # Tick de banco desacoplado da entrega de tempo real.
                now_monotonic = time.monotonic()
                try:
                    if (now_monotonic - last_db_tick_monotonic) >= DB_TICK_INTERVAL_S:
                        if readings_buffer.should_flush():
                            await asyncio.to_thread(readings_buffer.flush)

                        if cycle_detector.has_pending_db_sync() and (
                            start_event
                            or end_event
                            or (now_monotonic - last_sync_monotonic) >= DB_SYNC_INTERVAL_S
                        ):
                            await asyncio.to_thread(cycle_detector.sync_db, readings_buffer)
                            last_sync_monotonic = time.monotonic()

                        await asyncio.to_thread(
                            cycle_detector.persist_state,
                            start_event or end_event,
                        )
                        last_db_tick_monotonic = time.monotonic()
                except Exception as exc:
                    logger.warning("Falha no tick de persistencia/flush: {}", exc)

                elapsed = time.monotonic() - loop_started_monotonic
                sleep_for = POLL_SECONDS - elapsed
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)

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
            logger.bind(component="poller", event="modbus_failure").error(
                "Falha de comunicacao Modbus: {}",
                exc,
            )
            await broadcaster(
                {
                    "_status": "modbus_down",
                    "_modbus_status": "FAIL",
                    "_db_status": readings_buffer.db_status,
                    "_error": str(exc),
                }
            )
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.to_thread(ensure_compostos_schema)
    except Exception as exc:
        logger.warning("Falha ao garantir schema de compostos: {}", exc)
        readings_buffer.mark_db_down("schema_compostos", exc)

    try:
        await asyncio.to_thread(ensure_cycle_tracking_schema)
    except Exception as exc:
        logger.warning("Falha ao garantir schema de ciclo: {}", exc)
        readings_buffer.mark_db_down("schema_cycle", exc)

    try:
        await asyncio.to_thread(cycle_detector.startup)
    except Exception as exc:
        readings_buffer.mark_db_down("cycle_startup", exc)
        logger.warning("Falha no startup do detector de ciclo: {}", exc)
    else:
        readings_buffer.mark_db_up("cycle_startup")

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


def _serialize_metrics(metrics: dict) -> dict:
    out = dict(metrics)
    for key in ("start_ts", "end_ts"):
        value = out.get(key)
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


def _serialize_events(events: list[dict]) -> list[dict]:
    out = []
    for event in events:
        payload = dict(event)
        ts = payload.get("ts")
        if isinstance(ts, datetime):
            payload["ts"] = ts.isoformat()
        out.append(payload)
    return out


def _serialize_meta(meta: dict) -> dict:
    out = dict(meta or {})
    for key in ("start_ts", "end_ts", "created_at"):
        value = out.get(key)
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


def _iso_or_none(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _invalidate_runtime_caches(batch_id: int | None = None) -> None:
    with _batch_payload_cache_lock:
        if batch_id is None:
            _batch_payload_cache.clear()
        else:
            try:
                _batch_payload_cache.pop(int(batch_id), None)
            except Exception:
                pass
        _batch_summary_cache.clear()


def _load_batch_payload_uncached(batch_id: int) -> tuple[dict, list[dict], list[dict], dict]:
    readings = fetch_batch_readings(batch_id)
    if not readings:
        raise HTTPException(
            status_code=404,
            detail=f"Batch {batch_id} sem leituras associadas no monitor.",
        )

    meta = fetch_batch_meta(batch_id)
    if not meta:
        # Fallback para batches legados/backfill sem linha em FIELDLOGGER_BATCHES.
        meta = {
            "batch_id": int(batch_id),
            "start_ts": readings[0].get("ts"),
            "end_ts": readings[-1].get("ts"),
            "duration_s": None,
            "max_temp": None,
            "max_corrente": None,
            "avg_temp": None,
            "avg_corrente": None,
            "signature": None,
            "op": None,
            "operador": None,
            "observacoes": "meta ausente em FIELDLOGGER_BATCHES (gerado por fallback)",
            "created_at": None,
        }

    events = derive_batch_events(readings, batch_meta=meta)
    metrics = compute_batch_metrics(readings, events=events, batch_meta=meta)

    if not meta.get("start_ts"):
        meta["start_ts"] = metrics.get("start_ts")
    if not meta.get("end_ts"):
        meta["end_ts"] = metrics.get("end_ts")

    return meta, readings, events, metrics


def _load_batch_payload(batch_id: int) -> tuple[dict, list[dict], list[dict], dict]:
    lookup_id = int(batch_id)
    now_monotonic = time.monotonic()

    with _batch_payload_cache_lock:
        cached = _batch_payload_cache.get(lookup_id)
        if cached and (now_monotonic - cached[0]) <= BATCH_METRICS_CACHE_TTL_S:
            return cached[1]

    payload = _load_batch_payload_uncached(lookup_id)
    with _batch_payload_cache_lock:
        _batch_payload_cache[lookup_id] = (time.monotonic(), payload)
    return payload


def _summary_cache_key(
    start_ts: datetime | None,
    end_ts: datetime | None,
    lote: str | None,
    composto: str | None,
    batch_id: str | None,
    op: str | None,
) -> str:
    parts = [
        start_ts.isoformat() if isinstance(start_ts, datetime) else "",
        end_ts.isoformat() if isinstance(end_ts, datetime) else "",
        (lote or "").strip().upper(),
        (composto or "").strip().upper(),
        (batch_id or "").strip().upper(),
        (op or "").strip().upper(),
    ]
    return "|".join(parts)


def _first_non_empty(values) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _build_batch_summary_payload(
    start_ts: datetime | None,
    end_ts: datetime | None,
    lote: str | None,
    composto: str | None,
    batch_id: str | None,
    op: str | None,
) -> dict:
    cache_key = _summary_cache_key(
        start_ts=start_ts,
        end_ts=end_ts,
        lote=lote,
        composto=composto,
        batch_id=batch_id,
        op=op,
    )
    now_monotonic = time.monotonic()
    with _batch_payload_cache_lock:
        cached = _batch_summary_cache.get(cache_key)
        if cached and (now_monotonic - cached[0]) <= BATCH_SUMMARY_CACHE_TTL_S:
            return cached[1]

    index_payload = fetch_history_batch_index(
        start_ts=start_ts,
        end_ts=end_ts,
        lote=lote,
        composto=composto,
        batch_id=batch_id,
        op=op,
    )

    summary_rows = []
    for index_row in index_payload.get("rows", []):
        raw_batch_id = index_row.get("batch_id")
        try:
            numeric_batch_id = int(raw_batch_id)
        except (TypeError, ValueError):
            continue

        try:
            meta, readings, events, metrics = _load_batch_payload(batch_id=numeric_batch_id)
        except HTTPException as exc:
            if exc.status_code == 404:
                continue
            raise

        lote_value = _first_non_empty(
            [index_row.get("lote")] + [row.get("lote") for row in readings]
        )
        composto_value = _first_non_empty(
            [index_row.get("composto")] + [row.get("composto") for row in readings]
        )
        op_value = _first_non_empty(
            [meta.get("op"), index_row.get("op")] + [row.get("op") for row in readings]
        )

        summary_rows.append(
            {
                "batch_id": numeric_batch_id,
                "start_ts": _iso_or_none(metrics.get("start_ts") or meta.get("start_ts")),
                "end_ts": _iso_or_none(metrics.get("end_ts") or meta.get("end_ts")),
                "duration_s": metrics.get("duration_s"),
                "samples": metrics.get("samples"),
                "min_temp": metrics.get("min_temp"),
                "max_temp": metrics.get("max_temp"),
                "avg_temp": metrics.get("avg_temp"),
                "min_corrente": metrics.get("min_corrente"),
                "max_corrente": metrics.get("max_corrente"),
                "avg_corrente": metrics.get("avg_corrente"),
                "current_auc": metrics.get("current_auc"),
                "energy_proxy": metrics.get("energy_proxy"),
                "energy_est_enabled": metrics.get("energy_est_enabled"),
                "energy_label": metrics.get("energy_label"),
                "vll_volts": metrics.get("vll_volts"),
                "power_factor_assumed": metrics.get("power_factor_assumed"),
                "power_est_avg_kw": metrics.get("power_est_avg_kw"),
                "power_est_peak_kw": metrics.get("power_est_peak_kw"),
                "energy_est_wh": metrics.get("energy_est_wh"),
                "energy_est_kwh": metrics.get("energy_est_kwh"),
                # Chaves antigas mantidas por compatibilidade.
                "energy_estimated_wh": metrics.get("energy_estimated_wh"),
                "energy_estimated_kwh": metrics.get("energy_estimated_kwh"),
                "composto": composto_value,
                "lote": lote_value,
                "op": op_value,
                "operador": meta.get("operador"),
                "events_count": len(events),
            }
        )

    payload = {
        "rows": summary_rows,
        "filters": index_payload.get("filters", {}),
        "meta": {
            "total_batches": len(summary_rows),
            "generated_at": datetime.utcnow().isoformat(),
        },
    }
    with _batch_payload_cache_lock:
        _batch_summary_cache[cache_key] = (time.monotonic(), payload)
    return payload


def _report_filename(batch_id: int, ext: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    return f"batch_{batch_id}_{stamp}.{ext}"


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


@app.get("/api/batches/summary")
def batches_summary(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    lote: str | None = Query(default=None),
    composto: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    op: str | None = Query(default=None),
):
    start_ts = _parse_history_datetime(start, "start")
    end_ts = _parse_history_datetime(end, "end")
    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(
            status_code=422,
            detail="Parametro invalido: 'start' deve ser menor ou igual a 'end'.",
        )

    return _build_batch_summary_payload(
        start_ts=start_ts,
        end_ts=end_ts,
        lote=lote,
        composto=composto,
        batch_id=batch_id,
        op=op,
    )


@app.get("/api/batches/summary/export")
def batches_summary_export(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    lote: str | None = Query(default=None),
    composto: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    op: str | None = Query(default=None),
):
    start_ts = _parse_history_datetime(start, "start")
    end_ts = _parse_history_datetime(end, "end")
    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(
            status_code=422,
            detail="Parametro invalido: 'start' deve ser menor ou igual a 'end'.",
        )

    payload = _build_batch_summary_payload(
        start_ts=start_ts,
        end_ts=end_ts,
        lote=lote,
        composto=composto,
        batch_id=batch_id,
        op=op,
    )
    content = build_batch_summary_excel(payload.get("rows", []))
    filename = f"batches_resumo_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return StreamingResponse(io.BytesIO(content), media_type=media_type, headers=headers)


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
    _invalidate_runtime_caches(batch_id=batch_id)
    return result


@app.get("/api/batches/{batch_id}/metrics")
def batch_metrics(batch_id: int):
    meta, _, events, metrics = _load_batch_payload(batch_id=batch_id)
    return {
        "batch_id": int(batch_id),
        "meta": _serialize_meta(meta),
        "metrics": _serialize_metrics(metrics),
        "events": _serialize_events(events),
    }


@app.get("/api/batches/{batch_id}/report")
def batch_report_download(
    batch_id: int,
    format: Literal["pdf", "xlsx"] = Query(default="pdf"),
    include_raw: int = Query(default=0, ge=0, le=1),
):
    meta, readings, events, metrics = _load_batch_payload(batch_id=batch_id)
    include_raw_bool = int(include_raw) == 1

    if format == "xlsx":
        content = build_excel_report(
            batch_id=batch_id,
            meta=meta,
            readings=readings,
            events=events,
            metrics=metrics,
            include_raw=include_raw_bool,
        )
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = _report_filename(batch_id=batch_id, ext="xlsx")
    else:
        content = build_pdf_report(
            batch_id=batch_id,
            meta=meta,
            readings=readings,
            events=events,
            metrics=metrics,
        )
        media_type = "application/pdf"
        filename = _report_filename(batch_id=batch_id, ext="pdf")

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(io.BytesIO(content), media_type=media_type, headers=headers)


@app.get("/api/batches/{batch_id}/signature")
def batch_signature(batch_id: int):
    meta, readings, _, _ = _load_batch_payload(batch_id=batch_id)
    signature_payload = build_signature_payload(readings)

    existing_signature = str(meta.get("signature") or "").strip()
    if not existing_signature:
        try:
            update_batch_signature_if_empty(
                batch_id=batch_id,
                signature=signature_payload["signature"],
            )
        except Exception as exc:
            logger.warning("Falha ao atualizar assinatura do batch {}: {}", batch_id, exc)

    return {
        "batch_id": batch_id,
        "signature": signature_payload["signature"],
        "profile": signature_payload["profile"],
    }


@app.get("/api/batches/compare")
def compare_batches(
    baseline_batch_id: int = Query(...),
    target_batch_id: int = Query(...),
):
    baseline_meta, baseline_readings, baseline_events, baseline_metrics = _load_batch_payload(
        batch_id=baseline_batch_id
    )
    target_meta, target_readings, target_events, target_metrics = _load_batch_payload(
        batch_id=target_batch_id
    )

    baseline_signature_payload = build_signature_payload(baseline_readings)
    target_signature_payload = build_signature_payload(target_readings)

    baseline_profile = baseline_signature_payload["profile"]
    target_profile = target_signature_payload["profile"]

    return {
        "baseline_batch_id": baseline_batch_id,
        "target_batch_id": target_batch_id,
        "baseline": {
            "batch_id": baseline_batch_id,
            "signature": baseline_signature_payload["signature"],
            "profile": baseline_profile,
            "metrics": _serialize_metrics(baseline_metrics),
            "events": _serialize_events(baseline_events),
            "meta": {
                "start_ts": baseline_meta.get("start_ts").isoformat()
                if isinstance(baseline_meta.get("start_ts"), datetime)
                else None,
                "end_ts": baseline_meta.get("end_ts").isoformat()
                if isinstance(baseline_meta.get("end_ts"), datetime)
                else None,
                "op": baseline_meta.get("op"),
                "operador": baseline_meta.get("operador"),
            },
        },
        "target": {
            "batch_id": target_batch_id,
            "signature": target_signature_payload["signature"],
            "profile": target_profile,
            "metrics": _serialize_metrics(target_metrics),
            "events": _serialize_events(target_events),
            "meta": {
                "start_ts": target_meta.get("start_ts").isoformat()
                if isinstance(target_meta.get("start_ts"), datetime)
                else None,
                "end_ts": target_meta.get("end_ts").isoformat()
                if isinstance(target_meta.get("end_ts"), datetime)
                else None,
                "op": target_meta.get("op"),
                "operador": target_meta.get("operador"),
            },
        },
        "delta_profile": build_delta_profile(
            baseline_profile=baseline_profile,
            target_profile=target_profile,
        ),
        "deviation_metrics": compute_compare_metrics(
            baseline_profile=baseline_profile,
            target_profile=target_profile,
            baseline_metrics=baseline_metrics,
            target_metrics=target_metrics,
        ),
    }


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
    fl = FieldLoggerModbus(
        MODBUS_HOST,
        MODBUS_PORT,
        MODBUS_UNIT_ID,
        timeout=MODBUS_TIMEOUT_S,
    )
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
