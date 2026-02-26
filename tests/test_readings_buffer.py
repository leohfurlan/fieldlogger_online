from readings_buffer import ReadingsBuffer


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _conn_factory():
    return DummyConn()


def test_flush_failure_keeps_rows_and_marks_db_down():
    def failing_bulk_insert(conn, rows):
        raise RuntimeError("oracle indisponivel")

    buffer = ReadingsBuffer(
        max_batch_size=2,
        flush_interval_s=0.1,
        max_queue_size=10,
        conn_factory=_conn_factory,
        bulk_insert_fn=failing_bulk_insert,
    )

    buffer.append({"id": 1})
    buffer.append({"id": 2})
    inserted = buffer.flush(force=True)

    assert inserted == 0
    assert buffer.db_status == "DOWN"
    assert buffer.pending_count == 2
    assert buffer.failure_count == 1


def test_queue_limit_discards_oldest_rows():
    inserted_rows = []

    def ok_bulk_insert(conn, rows):
        inserted_rows.extend(rows)
        return len(rows)

    buffer = ReadingsBuffer(
        max_batch_size=100,
        flush_interval_s=0.1,
        max_queue_size=3,
        conn_factory=_conn_factory,
        bulk_insert_fn=ok_bulk_insert,
    )

    for row_id in range(1, 6):
        buffer.append({"id": row_id})

    assert buffer.pending_count == 3
    assert buffer.dropped_rows == 2

    inserted = buffer.flush(force=True)
    assert inserted == 3
    assert [r["id"] for r in inserted_rows] == [3, 4, 5]
    assert buffer.db_status == "UP"
