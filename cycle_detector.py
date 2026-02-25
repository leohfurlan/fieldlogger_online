import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from config import logger


@dataclass
class CycleStats:
    sample_every: int = 10
    count: int = 0
    sample_index: int = 0
    sum_temp: float = 0.0
    sum_corrente: float = 0.0
    max_temp: Optional[float] = None
    max_corrente: Optional[float] = None
    samples: list[str] = field(default_factory=list)

    def add(self, temperatura: float | None, corrente: float | None) -> None:
        temp_v = _to_float_or_none(temperatura)
        corr_v = _to_float_or_none(corrente)

        if temp_v is not None:
            self.sum_temp += temp_v
            self.max_temp = temp_v if self.max_temp is None else max(self.max_temp, temp_v)

        if corr_v is not None:
            self.sum_corrente += corr_v
            self.max_corrente = corr_v if self.max_corrente is None else max(self.max_corrente, corr_v)

        self.count += 1
        if self.sample_every <= 1 or (self.sample_index % self.sample_every) == 0:
            self.samples.append(f"{_fmt2(temp_v)}|{_fmt2(corr_v)}")
        self.sample_index += 1

    def avg_temp(self) -> float | None:
        if self.count <= 0:
            return None
        return self.sum_temp / self.count

    def avg_corrente(self) -> float | None:
        if self.count <= 0:
            return None
        return self.sum_corrente / self.count

    def signature(self, duration_s: float | None) -> str:
        base = {
            "duration_s": None if duration_s is None else round(float(duration_s), 3),
            "samples": self.samples,
            "count": self.count,
        }
        encoded = str(base).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:64]

    def to_state(self) -> dict:
        return {
            "sample_every": self.sample_every,
            "count": self.count,
            "sample_index": self.sample_index,
            "sum_temp": self.sum_temp,
            "sum_corrente": self.sum_corrente,
            "max_temp": self.max_temp,
            "max_corrente": self.max_corrente,
            "samples": list(self.samples),
        }

    @classmethod
    def from_state(cls, payload: dict | None, sample_every_default: int = 10) -> "CycleStats":
        if not isinstance(payload, dict):
            return cls(sample_every=sample_every_default)

        stats = cls(sample_every=int(payload.get("sample_every") or sample_every_default))
        stats.count = int(payload.get("count") or 0)
        stats.sample_index = int(payload.get("sample_index") or stats.count)
        stats.sum_temp = float(payload.get("sum_temp") or 0.0)
        stats.sum_corrente = float(payload.get("sum_corrente") or 0.0)

        max_temp = payload.get("max_temp")
        stats.max_temp = float(max_temp) if max_temp is not None else None
        max_corrente = payload.get("max_corrente")
        stats.max_corrente = float(max_corrente) if max_corrente is not None else None

        samples = payload.get("samples")
        if isinstance(samples, list):
            stats.samples = [str(item) for item in samples]
        return stats


@dataclass
class CycleRecord:
    token: str
    start_ts: datetime
    batch_id: int | None
    stats: CycleStats
    closed: bool = False
    end_ts: datetime | None = None
    db_closed: bool = False
    observacoes_close: str | None = None


@dataclass
class CycleDecision:
    batch_id: int | None
    cycle_token: str | None
    start_event: bool
    end_event: bool


class CycleLifecycleManager:
    def __init__(
        self,
        db_layer,
        state_key: str = "cycle_detector_state",
        state_persist_interval_s: float = 10.0,
        stale_cycle_timeout_s: float = 2 * 60 * 60,
        signature_sample_every: int = 10,
    ) -> None:
        self.db = db_layer
        self.state_key = state_key
        self.state_persist_interval_s = max(1.0, float(state_persist_interval_s))
        self.stale_cycle_timeout_s = max(1.0, float(stale_cycle_timeout_s))
        self.signature_sample_every = max(1, int(signature_sample_every))

        self.active_cycle: CycleRecord | None = None
        self.pending_cycles: list[CycleRecord] = []

        self.last_start_signal: int | None = None
        self.last_lid_signal: int | None = None
        self.last_read_ts: datetime | None = None

        self._state_dirty = True
        self._last_state_persist_monotonic = 0.0
        self._persist_retry_not_before_monotonic = 0.0
        self._persist_retry_backoff_s = 1.0
        self._max_persist_retry_backoff_s = 30.0
        self._last_persist_error_log_monotonic = 0.0
        self._persist_error_log_interval_s = 15.0
        self._sync_retry_not_before_monotonic = 0.0
        self._sync_retry_backoff_s = 1.0
        self._max_sync_retry_backoff_s = 30.0
        self._last_sync_error_log_monotonic = 0.0
        self._sync_error_log_interval_s = 15.0

    def startup(self) -> dict:
        state = None
        try:
            with self.db.get_conn() as conn:
                state = self.db.get_state(conn, self.state_key)
        except Exception as exc:
            logger.warning("Falha ao carregar estado do detector no startup: {}", exc)

        auto_closed = False
        if isinstance(state, dict):
            self._restore_from_state(state)
            auto_closed = self._autoclose_if_stale()

        self.persist_state(force=True)
        return {"loaded": isinstance(state, dict), "auto_closed": auto_closed}

    def shutdown(self) -> None:
        self.persist_state(force=True)

    def process_reading(self, reading: dict, ts: datetime | None = None) -> CycleDecision:
        read_ts = ts or datetime.utcnow()
        start_signal = _as_bit(reading.get("botao_start"))
        lid_signal = _as_bit(reading.get("tampa_descarga"))

        start_event = False
        end_event = False

        if self.active_cycle is None:
            should_start = False
            if self.last_start_signal is None and self.last_lid_signal is None:
                should_start = start_signal == 1 and lid_signal == 1
            elif self.last_start_signal == 0 and start_signal == 1:
                should_start = True

            if should_start:
                self.active_cycle = CycleRecord(
                    token=self._new_cycle_token(),
                    start_ts=read_ts,
                    batch_id=None,
                    stats=CycleStats(sample_every=self.signature_sample_every),
                )
                start_event = True
                self._state_dirty = True

        batch_id_for_row = self.active_cycle.batch_id if self.active_cycle else None
        cycle_token_for_row = self.active_cycle.token if self.active_cycle else None

        if self.active_cycle:
            self.active_cycle.stats.add(
                temperatura=reading.get("temperatura_term"),
                corrente=reading.get("corrente"),
            )

        lid_open_edge = self.last_lid_signal == 1 and lid_signal == 0
        if self.active_cycle and lid_open_edge:
            self.active_cycle.closed = True
            self.active_cycle.end_ts = read_ts
            self.pending_cycles.append(self.active_cycle)
            self.active_cycle = None
            end_event = True
            self._state_dirty = True

        self.last_start_signal = start_signal
        self.last_lid_signal = lid_signal
        self.last_read_ts = read_ts

        return CycleDecision(
            batch_id=batch_id_for_row,
            cycle_token=cycle_token_for_row,
            start_event=start_event,
            end_event=end_event,
        )

    def sync_db(self, readings_buffer) -> bool:
        targets: list[CycleRecord] = []
        if self.active_cycle is not None:
            targets.append(self.active_cycle)
        targets.extend(self.pending_cycles)

        if not targets:
            return True

        created_ids: dict[str, int] = {}
        closed_tokens: set[str] = set()
        now = time.monotonic()
        if now < self._sync_retry_not_before_monotonic:
            return False

        try:
            with self.db.get_conn() as conn:
                for cycle in targets:
                    resolved_batch_id = cycle.batch_id
                    if resolved_batch_id is None:
                        resolved_batch_id = int(self.db.create_batch(conn, start_ts=cycle.start_ts))
                        created_ids[cycle.token] = resolved_batch_id

                    if cycle.closed and not cycle.db_closed and cycle.end_ts is not None:
                        duration_s = max(0.0, (cycle.end_ts - cycle.start_ts).total_seconds())
                        self.db.close_batch(
                            conn,
                            batch_id=int(resolved_batch_id),
                            end_ts=cycle.end_ts,
                            duration_s=duration_s,
                            max_temp=cycle.stats.max_temp,
                            max_corrente=cycle.stats.max_corrente,
                            avg_temp=cycle.stats.avg_temp(),
                            avg_corrente=cycle.stats.avg_corrente(),
                            signature=cycle.stats.signature(duration_s=duration_s),
                            observacoes=cycle.observacoes_close,
                        )
                        closed_tokens.add(cycle.token)
                conn.commit()
        except Exception as exc:
            self._sync_retry_not_before_monotonic = now + self._sync_retry_backoff_s
            self._sync_retry_backoff_s = min(
                self._sync_retry_backoff_s * 2.0,
                self._max_sync_retry_backoff_s,
            )
            should_log = (
                self._last_sync_error_log_monotonic <= 0
                or (now - self._last_sync_error_log_monotonic)
                >= self._sync_error_log_interval_s
            )
            if should_log:
                self._last_sync_error_log_monotonic = now
                logger.warning("Falha ao sincronizar ciclo com Oracle: {}", exc)
            return False

        self._sync_retry_not_before_monotonic = 0.0
        self._sync_retry_backoff_s = 1.0
        for cycle in targets:
            if cycle.token in created_ids:
                cycle.batch_id = created_ids[cycle.token]
                readings_buffer.assign_batch_id(cycle.token, cycle.batch_id)
                self._state_dirty = True

        if closed_tokens:
            for cycle in self.pending_cycles:
                if cycle.token in closed_tokens:
                    cycle.db_closed = True
                    self._state_dirty = True
            self.pending_cycles = [cycle for cycle in self.pending_cycles if not cycle.db_closed]

        return True

    def batch_id_for_token(self, cycle_token: str | None) -> int | None:
        if not cycle_token:
            return None
        if self.active_cycle and self.active_cycle.token == cycle_token:
            return self.active_cycle.batch_id
        for cycle in self.pending_cycles:
            if cycle.token == cycle_token:
                return cycle.batch_id
        return None

    def persist_state(self, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and now < self._persist_retry_not_before_monotonic:
            return False

        should_persist = force or self._state_dirty

        if not should_persist and self.active_cycle is not None:
            elapsed = time.monotonic() - self._last_state_persist_monotonic
            should_persist = elapsed >= self.state_persist_interval_s

        if not should_persist:
            return False

        payload = self._serialize_state()
        try:
            with self.db.get_conn() as conn:
                self.db.set_state(conn, self.state_key, payload)
                conn.commit()
        except Exception as exc:
            self._persist_retry_not_before_monotonic = now + self._persist_retry_backoff_s
            self._persist_retry_backoff_s = min(
                self._persist_retry_backoff_s * 2.0,
                self._max_persist_retry_backoff_s,
            )
            should_log = (
                force
                or self._last_persist_error_log_monotonic <= 0
                or (now - self._last_persist_error_log_monotonic)
                >= self._persist_error_log_interval_s
            )
            if should_log:
                self._last_persist_error_log_monotonic = now
                logger.warning("Falha ao persistir estado do detector: {}", exc)
            return False

        self._state_dirty = False
        self._last_state_persist_monotonic = time.monotonic()
        self._persist_retry_not_before_monotonic = 0.0
        self._persist_retry_backoff_s = 1.0
        return True

    def _restore_from_state(self, state: dict) -> None:
        self.last_start_signal = _as_bit(state.get("last_start_signal"))
        self.last_lid_signal = _as_bit(state.get("last_lid_signal"))
        self.last_read_ts = _parse_iso(state.get("last_read_ts"))

        is_running = bool(state.get("is_running"))
        if not is_running:
            return

        start_ts = _parse_iso(state.get("start_ts"))
        if start_ts is None:
            start_ts = datetime.utcnow()

        batch_id = state.get("current_batch_id")
        try:
            batch_id = int(batch_id) if batch_id is not None else None
        except Exception:
            batch_id = None

        stats = CycleStats.from_state(
            state.get("stats"),
            sample_every_default=self.signature_sample_every,
        )

        token = str(state.get("cycle_token") or self._new_cycle_token())

        self.active_cycle = CycleRecord(
            token=token,
            start_ts=start_ts,
            batch_id=batch_id,
            stats=stats,
        )
        self._state_dirty = True

    def _serialize_state(self) -> dict:
        active = self.active_cycle
        return {
            "is_running": active is not None,
            "current_batch_id": active.batch_id if active else None,
            "last_start_signal": self.last_start_signal,
            "last_lid_signal": self.last_lid_signal,
            "start_ts": active.start_ts.isoformat() if active else None,
            "last_read_ts": self.last_read_ts.isoformat() if self.last_read_ts else None,
            "cycle_token": active.token if active else None,
            "stats": active.stats.to_state() if active else None,
            "pending_cycles": len(self.pending_cycles),
        }

    def _autoclose_if_stale(self) -> bool:
        cycle = self.active_cycle
        if cycle is None or cycle.batch_id is None:
            return False
        if self.last_read_ts is None:
            return False

        age_s = (datetime.utcnow() - self.last_read_ts).total_seconds()
        if age_s <= self.stale_cycle_timeout_s:
            return False

        try:
            self.db.close_batch_auto_after_downtime(
                batch_id=int(cycle.batch_id),
                end_ts=self.last_read_ts,
                observacoes="auto-close after downtime",
            )
        except Exception as exc:
            logger.warning("Falha no auto-close de batch apos downtime: {}", exc)
            return False

        self.active_cycle = None
        self._state_dirty = True
        return True

    @staticmethod
    def _new_cycle_token() -> str:
        return uuid.uuid4().hex[:16]


def _fmt2(value: float | None) -> str:
    if value is None:
        return "null"
    return f"{float(value):.2f}"


def _to_float_or_none(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_bit(value) -> int:
    if value is None:
        return 0
    try:
        return 1 if int(value) > 0 else 0
    except Exception:
        return 0


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
