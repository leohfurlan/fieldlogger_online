from datetime import datetime, timedelta

from cycle_detector import CycleLifecycleManager


class FakeConn:
    def __init__(self):
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def commit(self):
        self.commits += 1


class FakeDb:
    def __init__(self):
        self.state = {}
        self.next_batch_id = 1
        self.created_batches = []
        self.closed_batches = []

    def get_conn(self):
        return FakeConn()

    def get_state(self, conn, key):
        return self.state.get(key)

    def set_state(self, conn, key, dict_value):
        self.state[key] = dict(dict_value)

    def create_batch(self, conn, start_ts, op=None, operador=None, observacoes=None):
        batch_id = self.next_batch_id
        self.next_batch_id += 1
        self.created_batches.append(
            {
                "batch_id": batch_id,
                "start_ts": start_ts,
                "op": op,
                "operador": operador,
                "observacoes": observacoes,
            }
        )
        return batch_id

    def close_batch(
        self,
        conn,
        batch_id,
        end_ts,
        duration_s,
        max_temp,
        max_corrente,
        avg_temp,
        avg_corrente,
        signature,
        observacoes=None,
    ):
        self.closed_batches.append(
            {
                "batch_id": batch_id,
                "end_ts": end_ts,
                "duration_s": duration_s,
                "max_temp": max_temp,
                "max_corrente": max_corrente,
                "avg_temp": avg_temp,
                "avg_corrente": avg_corrente,
                "signature": signature,
                "observacoes": observacoes,
            }
        )
        return 1

    def close_batch_auto_after_downtime(self, batch_id, end_ts, observacoes):
        return {"updated_rows": 1, "batch_id": batch_id, "end_ts": end_ts, "observacoes": observacoes}


class FakeBuffer:
    def __init__(self):
        self.assignments = []

    def assign_batch_id(self, cycle_token, batch_id):
        self.assignments.append((cycle_token, batch_id))
        return 1


def test_cycle_start_end_and_state_persistence():
    db = FakeDb()
    buffer = FakeBuffer()
    manager = CycleLifecycleManager(
        db_layer=db,
        state_key="cycle_detector_state",
        state_persist_interval_s=10,
        stale_cycle_timeout_s=7200,
        signature_sample_every=2,
    )

    manager.startup()

    t0 = datetime(2026, 2, 25, 8, 0, 0)
    t1 = t0 + timedelta(seconds=1)
    t2 = t0 + timedelta(seconds=2)
    t3 = t0 + timedelta(seconds=3)

    d0 = manager.process_reading(
        {"botao_start": 0, "tampa_descarga": 1, "temperatura_term": 95.0, "corrente": 4.0},
        ts=t0,
    )
    assert d0.start_event is False
    assert d0.end_event is False

    d1 = manager.process_reading(
        {"botao_start": 1, "tampa_descarga": 1, "temperatura_term": 110.0, "corrente": 8.0},
        ts=t1,
    )
    assert d1.start_event is True
    assert d1.end_event is False

    assert manager.sync_db(buffer) is True
    assert len(db.created_batches) == 1
    assert d1.cycle_token is not None
    assert manager.batch_id_for_token(d1.cycle_token) == 1
    assert buffer.assignments == [(d1.cycle_token, 1)]

    persisted_start = manager.persist_state(force=True)
    assert persisted_start is True
    state_after_start = db.state["cycle_detector_state"]
    assert state_after_start["is_running"] is True
    assert state_after_start["current_batch_id"] == 1
    assert state_after_start["last_start_signal"] == 1
    assert state_after_start["last_lid_signal"] == 1

    d2 = manager.process_reading(
        {"botao_start": 1, "tampa_descarga": 1, "temperatura_term": 120.0, "corrente": 10.0},
        ts=t2,
    )
    assert d2.start_event is False
    assert d2.end_event is False

    d3 = manager.process_reading(
        {"botao_start": 1, "tampa_descarga": 0, "temperatura_term": 130.0, "corrente": 12.0},
        ts=t3,
    )
    assert d3.start_event is False
    assert d3.end_event is True

    assert manager.sync_db(buffer) is True
    assert len(db.closed_batches) == 1
    closed = db.closed_batches[0]
    assert closed["batch_id"] == 1
    assert closed["duration_s"] == 2.0
    assert closed["max_temp"] == 130.0
    assert closed["max_corrente"] == 12.0
    assert closed["avg_temp"] is not None
    assert closed["avg_corrente"] is not None
    assert isinstance(closed["signature"], str)
    assert len(closed["signature"]) == 64

    persisted_end = manager.persist_state(force=True)
    assert persisted_end is True
    state_after_end = db.state["cycle_detector_state"]
    assert state_after_end["is_running"] is False
    assert state_after_end["current_batch_id"] is None
    assert state_after_end["last_start_signal"] == 1
    assert state_after_end["last_lid_signal"] == 0
    assert state_after_end["last_read_ts"] == t3.isoformat()
