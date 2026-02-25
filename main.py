# main.py
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import os
from pathlib import Path
from typing import Set
from dotenv import load_dotenv

load_dotenv()


from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

# Imports que funcionam tanto rodando "uvicorn main:app" dentro da pasta app
# quanto rodando "uvicorn app.main:app" a partir da pasta pai.
try:
    from db import (
        assign_composto_to_batch,
        ensure_batch_id_schema,
        ensure_compostos_schema,
        fetch_batch_runtime_state,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        insert_monitor_row,
        list_compostos_catalog,
        list_tables,
    )
    from modbus_client import FieldLoggerModbus
except ModuleNotFoundError:
    from db import (
        assign_composto_to_batch,
        ensure_batch_id_schema,
        ensure_compostos_schema,
        fetch_batch_runtime_state,
        fetch_history_filter_options,
        fetch_monitor_history,
        import_compostos_from_sankhya,
        insert_monitor_row,
        list_compostos_catalog,
        list_tables,
    )
    from modbus_client import FieldLoggerModbus

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

# -------- WS CLIENTS --------
ws_clients: Set[WebSocket] = set()


class BatchCycleDetector:
    """
    Detector de ciclo por borda:
      - borda de subida do botao_start (0->1) inicia batch
      - borda de descida da tampa_descarga (1->0) encerra batch

    Regra de tampa usada aqui (conforme sua configuracao):
      1 = tampa fechada
      0 = tampa aberta
    """

    def __init__(self):
        self.next_batch_id = 1
        self.current_batch_id: int | None = None
        self.cycle_active = False
        self.prev_start: int | None = None
        self.prev_tampa_descarga: int | None = None

    @staticmethod
    def _as_bit(value) -> int:
        try:
            return 1 if int(value) > 0 else 0
        except Exception:
            return 0

    def _open_new_batch(self) -> None:
        self.current_batch_id = self.next_batch_id
        self.next_batch_id += 1
        self.cycle_active = True

    def restore(self, state: dict) -> None:
        self.next_batch_id = max(1, int(state.get("next_batch_id") or 1))
        self.prev_start = (
            self._as_bit(state["last_start"])
            if state.get("last_start") is not None
            else None
        )
        self.prev_tampa_descarga = (
            self._as_bit(state["last_tampa_descarga"])
            if state.get("last_tampa_descarga") is not None
            else None
        )

        last_batch_id = state.get("last_batch_id")
        if isinstance(last_batch_id, int) and self.prev_tampa_descarga == 1:
            # ultima leitura foi com tampa fechada e batch em andamento
            self.current_batch_id = last_batch_id
            self.cycle_active = True
        else:
            self.current_batch_id = None
            self.cycle_active = False

    def process(self, botao_start, tampa_descarga) -> int | None:
        start = self._as_bit(botao_start)
        tampa = self._as_bit(tampa_descarga)

        start_rising = self.prev_start == 0 and start == 1
        # tampa abrindo: 1 (fechada) -> 0 (aberta)
        tampa_open_edge = self.prev_tampa_descarga == 1 and tampa == 0

        if not self.cycle_active:
            # primeira leitura com sistema ja em ciclo
            if self.prev_start is None and self.prev_tampa_descarga is None:
                if start == 1 and tampa == 1:
                    self._open_new_batch()
            elif start_rising:
                self._open_new_batch()

        batch_id_for_row = self.current_batch_id if self.cycle_active else None

        if self.cycle_active and tampa_open_edge:
            # linha atual ainda pertence ao batch; fechamento vale para a proxima
            self.cycle_active = False
            self.current_batch_id = None

        self.prev_start = start
        self.prev_tampa_descarga = tampa
        return batch_id_for_row


batch_cycle_detector = BatchCycleDetector()


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
      - lê registradores
      - grava no Oracle
      - envia ao vivo via WebSocket
    Tem reconexão automática se cair rede/modbus.
    """
    while True:
        fl = FieldLoggerModbus(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT_ID)
        try:
            if not await asyncio.to_thread(fl.connect):
                raise RuntimeError("Falha ao conectar no Modbus TCP (connect=False).")

            while True:
                data = await asyncio.to_thread(fl.read)
                batch_id = batch_cycle_detector.process(
                    data.get("botao_start"),
                    data.get("tampa_descarga"),
                )
                data["batch_id"] = batch_id

                # grava no Oracle (deixe o db.py fazer commit)
                try:
                    await asyncio.to_thread(
                        insert_monitor_row,
                        {
                            "temp_raw": data.get("temperatura_term_raw"),
                            "corrente_raw": data.get("corrente_raw"),
                            "temperatura_c": data.get("temperatura_term"),
                            "corrente": data.get("corrente"),
                            "botao_start": data.get("botao_start"),
                            "tampa_descarga": data.get("tampa_descarga"),
                            "batch_id": batch_id,
                            "src": f"{MODBUS_HOST}:{MODBUS_PORT}/u{MODBUS_UNIT_ID}",
                        },
                    )
                except Exception as e:
                    # Não derruba tempo real por falha de banco
                    data["_db_error"] = str(e)

                await broadcaster(data)
                await asyncio.sleep(POLL_SECONDS)

        except asyncio.CancelledError:
            # shutdown limpo
            try:
                await asyncio.to_thread(fl.close)
            except Exception:
                pass
            raise
        except Exception as e:
            # caiu modbus/rede: espera e tenta reconectar
            try:
                await asyncio.to_thread(fl.close)
            except Exception:
                pass
            await broadcaster({"_status": "modbus_down", "_error": str(e)})
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.to_thread(ensure_compostos_schema)
    except Exception:
        pass

    try:
        await asyncio.to_thread(ensure_batch_id_schema)
    except Exception:
        # Se nao conseguir alterar schema, app segue; insercao lida com fallback.
        pass

    try:
        runtime_state = await asyncio.to_thread(fetch_batch_runtime_state)
        batch_cycle_detector.restore(runtime_state)
    except Exception:
        pass

    task = asyncio.create_task(poller())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


def _parse_history_datetime(value: str | None, field_name: str) -> datetime | None:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    # Aceita ISO e também formato Oracle retornado pela aplicação.
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
                f"Parâmetro inválido: '{field_name}'. Use 'YYYY-MM-DD', "
                "'YYYY-MM-DDTHH:MM' ou 'YYYY-MM-DD HH:MM:SS'."
            ),
        ) from exc


@app.get("/", response_class=HTMLResponse)
def index():
    html_path = BASE_DIR / "templates" / "index.html"
    if not html_path.exists():
        return HTMLResponse(
            "<h3>index.html não encontrado em templates/</h3>", status_code=500
        )
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/historico", response_class=HTMLResponse)
def history_page():
    html_path = BASE_DIR / "templates" / "history.html"
    if not html_path.exists():
        return HTMLResponse(
            "<h3>history.html não encontrado em templates/</h3>", status_code=500
        )
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/history")
def history_data(
    start: str | None = Query(
        default=None,
        description="Início do período. Ex.: 2026-02-25T07:30 ou 2026-02-25 07:30:00",
    ),
    end: str | None = Query(
        default=None,
        description="Fim do período. Ex.: 2026-02-25T08:00 ou 2026-02-25 08:00:00",
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
            detail="Parâmetro inválido: 'start' deve ser menor ou igual a 'end'.",
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
            detail="Parâmetro inválido: 'start' deve ser menor ou igual a 'end'.",
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
        raise HTTPException(status_code=400, detail=result.get("reason", "Falha ao vincular composto."))
    return result


@app.get("/health/db")
def health_db():
    # Se conectar e listar, tá ok
    tables = list_tables("ENGENHARIA")
    return {"ok": True, "owner": "ENGENHARIA", "tables_count": len(tables)}


@app.get("/db/tables")
def db_tables():
    return {"owner": "ENGENHARIA", "tables": list_tables("ENGENHARIA")}


@app.get("/health/modbus")
def health_modbus():
    # check rápido (sem ficar preso)
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
            # Mantém a conexão aberta; dados chegam via broadcaster()
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)
