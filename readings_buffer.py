import threading
import time
from typing import Callable

from config import logger
from db import get_conn, insert_readings_bulk


class ReadingsBuffer:
    """
    Buffer de leituras para reduzir commits no Oracle com executemany.
    """

    def __init__(
        self,
        max_batch_size: int = 100,
        flush_interval_s: float = 2.0,
        conn_factory: Callable = get_conn,
        bulk_insert_fn: Callable = insert_readings_bulk,
    ) -> None:
        self.max_batch_size = max(1, int(max_batch_size))
        self.flush_interval_s = max(0.1, float(flush_interval_s))
        self._conn_factory = conn_factory
        self._bulk_insert_fn = bulk_insert_fn

        self._lock = threading.Lock()
        self._rows: list[dict] = []
        self._last_flush_monotonic = time.monotonic()
        self._failure_count = 0
        self._total_flushed = 0
        self._retry_not_before_monotonic = 0.0
        self._retry_backoff_s = 1.0
        self._max_retry_backoff_s = 30.0
        self._last_error_log_monotonic = 0.0
        self._error_log_interval_s = 15.0

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._rows)

    @property
    def total_flushed(self) -> int:
        with self._lock:
            return self._total_flushed

    def append(self, reading_dict: dict) -> None:
        with self._lock:
            self._rows.append(dict(reading_dict))

    def assign_batch_id(self, cycle_token: str, batch_id: int) -> int:
        if not cycle_token:
            return 0

        changed = 0
        with self._lock:
            for row in self._rows:
                if row.get("_cycle_token") == cycle_token:
                    row["batch_id"] = int(batch_id)
                    changed += 1
        return changed

    def should_flush(self, force: bool = False) -> bool:
        with self._lock:
            if not self._rows:
                return False
            if force:
                return True
            if len(self._rows) >= self.max_batch_size:
                return True
            return (time.monotonic() - self._last_flush_monotonic) >= self.flush_interval_s

    def flush(self, force: bool = False) -> int:
        rows_to_flush: list[dict] = []
        now = time.monotonic()

        with self._lock:
            if not self._rows:
                return 0
            if not force and now < self._retry_not_before_monotonic:
                return 0
            if not force and len(self._rows) < self.max_batch_size:
                elapsed = time.monotonic() - self._last_flush_monotonic
                if elapsed < self.flush_interval_s:
                    return 0
            rows_to_flush = self._rows
            self._rows = []

        sanitized_rows = []
        for row in rows_to_flush:
            payload = dict(row)
            payload.pop("_cycle_token", None)
            sanitized_rows.append(payload)

        try:
            with self._conn_factory() as conn:
                inserted = int(self._bulk_insert_fn(conn, sanitized_rows))
        except Exception as exc:
            with self._lock:
                self._rows = rows_to_flush + self._rows
                self._failure_count += 1
                self._retry_not_before_monotonic = now + self._retry_backoff_s
                self._retry_backoff_s = min(
                    self._retry_backoff_s * 2.0, self._max_retry_backoff_s
                )
                should_log = (
                    force
                    or self._last_error_log_monotonic <= 0
                    or (now - self._last_error_log_monotonic) >= self._error_log_interval_s
                )
                if should_log:
                    self._last_error_log_monotonic = now
                    logger.error(
                        "Falha no flush de leituras em lote. pendentes={} falhas={} erro={}",
                        len(rows_to_flush),
                        self._failure_count,
                        exc,
                    )
            return 0

        with self._lock:
            self._last_flush_monotonic = time.monotonic()
            self._total_flushed += inserted
            self._retry_not_before_monotonic = 0.0
            self._retry_backoff_s = 1.0
        return inserted
