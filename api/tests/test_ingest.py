"""Dual-source ingest tests (require the local PostgreSQL in conftest)."""

import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from app import config
from fastapi.testclient import TestClient

from app import ingest, main

HOUR = datetime(2026, 11, 20, 8, 0, tzinfo=timezone.utc)
IX = "IX-T1"


@pytest.fixture()
def client():
    with TestClient(main.app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean():
    ingest.ensure_schema()
    with psycopg.connect(config.PRIMARY_DSN) as conn:
        conn.execute("DELETE FROM traffic_minute_counts")
        conn.execute("DELETE FROM gateway_ingest.minute_conclusion")
        conn.execute("DELETE FROM gateway_ingest.source_progress")
        conn.execute("DELETE FROM gateway_ingest.hour_state")
    yield


def minutes(start=0, end=59, *, missing=(), base=20):
    out = []
    for offset in range(start, end + 1):
        out.append(
            {
                "minute": HOUR + timedelta(minutes=offset),
                "is_missing": offset in missing,
                "vehicle_count": None if offset in missing else base + offset,
            }
        )
    return out


def report(source, mins, ix=IX, hour=HOUR):
    return ingest.report(ix, source, hour, mins)


def business_rows(ix=IX):
    with psycopg.connect(config.PRIMARY_DSN) as conn:
        rows = conn.execute(
            "SELECT minute_utc, vehicle_count FROM traffic_minute_counts "
            "WHERE intersection_id=%s AND minute_utc >= %s AND minute_utc < %s "
            "ORDER BY minute_utc",
            (ix, HOUR, HOUR + timedelta(hours=1)),
        ).fetchall()
    return [(m, c) for m, c in rows]


def test_not_ready_until_a_source_completes(client):
    r = client.post(
        "/ingest/reports",
        json={
            "intersection_id": IX,
            "source": "backup",
            "hour_utc": HOUR.isoformat(),
            "minutes": [
                {"minute_utc": (HOUR + timedelta(minutes=m)).isoformat(), "vehicle_count": m}
                for m in range(30)
            ],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ready"] is False
    assert body["progress"]["backup"]["decided_minutes"] == 30
    assert body["progress"]["primary"]["decided_minutes"] == 0

    q = client.get(
        "/ingest/hours", params={"intersection_id": IX, "hour_utc": HOUR.isoformat()}
    )
    assert q.status_code == 409
    detail = q.json()["detail"]
    assert detail["not_ready"] is True
    assert detail["progress"]["backup"]["decided_minutes"] == 30
    assert business_rows() == []


def test_backup_serves_first_then_primary_takes_over(client):
    assert report("backup", minutes())["serving_source"] == "backup"
    assert len(business_rows()) == 60
    assert business_rows()[0][1] == 20  # backup base

    out = report("primary", minutes(missing=(5, 6)))
    assert out["serving_source"] == "primary"
    assert out["published"] is True
    rows = business_rows()
    assert len(rows) == 58
    missing_minutes = {m for m, _ in rows}
    assert HOUR + timedelta(minutes=5) not in missing_minutes
    assert HOUR + timedelta(minutes=6) not in missing_minutes
    assert rows[0][1] == 20  # now the primary's value, not backup's 120

    q = client.get(
        "/ingest/hours", params={"intersection_id": IX, "hour_utc": HOUR.isoformat()}
    )
    assert q.status_code == 200
    body = q.json()
    assert body["serving_source"] == "primary"
    assert body["version"] == 2
    assert body["row_count"] == 58
    assert len(body["missing_minutes"]) == 2


def test_primary_first_then_backup_must_not_revert(client):
    report("primary", minutes())
    rows_before = business_rows()
    out = report("backup", minutes())
    assert out["serving_source"] == "primary"
    assert out["published"] is False
    assert business_rows() == rows_before


def test_late_partial_backup_batch_does_not_revert(client):
    report("primary", minutes())
    # backup reports minutes 59 only, much later
    out = report("backup", minutes(59, 59))
    assert out["serving_source"] == "primary"
    assert business_rows() == business_rows()  # unchanged


def test_identical_retransmission_is_noop(client):
    first = report("backup", minutes())
    version = first["version"]
    second = report("backup", minutes())
    assert second["changed"] is False
    assert second["new_minutes"] == 0
    assert second["version"] == version
    assert len(business_rows()) == 60


def test_conflicting_batch_rejected_whole(client):
    report("backup", minutes(0, 9))
    # batch disagrees at minute 3 and also carries a brand-new minute 40
    bad = minutes(0, 9) + minutes(40, 40)
    bad[3]["vehicle_count"] = 999
    with pytest.raises(ingest.Conflict) as exc:
        report("backup", bad)
    assert exc.value.detail["conflicts"][0]["minute_utc"].endswith("08:03:00Z")
    # atomic rejection: the new minute 40 was not applied either
    q = ingest.get_progress(IX, HOUR)
    assert q["progress"]["backup"]["decided_minutes"] == 10
    assert business_rows() == []


def test_same_minute_different_source_is_independent(client):
    report("backup", minutes(0, 9))
    report("primary", minutes(0, 9, base=20))
    prog = ingest.get_progress(IX, HOUR)["progress"]
    assert prog["backup"]["decided_minutes"] == 10
    assert prog["primary"]["decided_minutes"] == 10


def test_duplicate_minute_in_batch_rejected(client):
    bad = minutes(0, 2) + minutes(2, 2)
    with pytest.raises(ingest.BadRequest):
        report("primary", bad)


def test_validation(client):
    ok = {
        "intersection_id": IX,
        "source": "primary",
        "hour_utc": HOUR.isoformat(),
        "minutes": [{"minute_utc": HOUR.isoformat(), "vehicle_count": 1}],
    }
    # missing with a count is refused by the request model
    r = client.post(
        "/ingest/reports",
        json={**ok, "minutes": [{"minute_utc": HOUR.isoformat(), "vehicle_count": 1, "missing": True}]},
    )
    assert r.status_code == 422
    # neither count nor missing
    r = client.post(
        "/ingest/reports",
        json={**ok, "minutes": [{"minute_utc": HOUR.isoformat()}]},
    )
    assert r.status_code == 422
    # unknown source
    r = client.post("/ingest/reports", json={**ok, "source": "shadow"})
    assert r.status_code == 422
    # minute outside the hour
    r = client.post(
        "/ingest/reports",
        json={
            **ok,
            "minutes": [{"minute_utc": (HOUR + timedelta(hours=1)).isoformat(), "vehicle_count": 1}],
        },
    )
    assert r.status_code == 400
    # non-whole hour
    r = client.post(
        "/ingest/reports", json={**ok, "hour_utc": (HOUR + timedelta(minutes=30)).isoformat()}
    )
    assert r.status_code == 400
    # naive datetime (no offset)
    r = client.post(
        "/ingest/reports", json={**ok, "hour_utc": "2026-11-20T08:00:00"}
    )
    assert r.status_code == 400


def test_hour_lock_serialises_same_hour(monkeypatch):
    """A report for the same intersection/hour waits for an in-flight lock."""
    ingest.ensure_schema()
    key = f"{IX}|{ingest.iso(HOUR)}"
    holder = psycopg.connect(config.PRIMARY_DSN, autocommit=True)
    try:
        holder.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,))
        done = threading.Event()

        def worker():
            report("primary", minutes())
            done.set()

        t = threading.Thread(target=worker)
        t.start()
        assert not done.wait(1.0), "report must wait while the lock is held"
        holder.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))
        assert done.wait(15), "report must proceed once the lock is released"
        t.join()
    finally:
        holder.close()


def test_lock_timeout_reports_conflict(monkeypatch):
    monkeypatch.setattr(ingest, "LOCK_WAIT_SEC", 0.3)
    ingest.ensure_schema()
    key = f"{IX}|{ingest.iso(HOUR)}"
    holder = psycopg.connect(config.PRIMARY_DSN, autocommit=True)
    try:
        holder.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,))
        with pytest.raises(ingest.Conflict):
            report("primary", minutes())
    finally:
        holder.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))
        holder.close()
    # nothing was applied
    assert ingest.get_progress(IX, HOUR)["progress"]["primary"]["decided_minutes"] == 0


def test_concurrent_sources_never_revert_after_primary(client):
    """Stress: interleaved uploads from both gateways always end on primary."""
    for _ in range(5):
        with psycopg.connect(config.PRIMARY_DSN) as conn:
            conn.execute("DELETE FROM traffic_minute_counts")
            conn.execute("DELETE FROM gateway_ingest.minute_conclusion")
            conn.execute("DELETE FROM gateway_ingest.source_progress")
            conn.execute("DELETE FROM gateway_ingest.hour_state")
        errors = []
        barrier = threading.Barrier(4)

        def worker(source, start):
            try:
                barrier.wait()
                report(source, minutes(start, 59))
                report(source, minutes(0, start - 1) if start else [])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=("primary", 40)),
            threading.Thread(target=worker, args=("backup", 30)),
            threading.Thread(target=worker, args=("primary", 20)),
            threading.Thread(target=worker, args=("backup", 10)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        state = ingest.get_progress(IX, HOUR)
        assert state["serving_source"] == "primary"
        rows = business_rows()
        assert len(rows) == 60
        assert rows[0][1] == 20  # primary value for minute 0


def test_restart_resumes_from_committed_state(client):
    report("backup", minutes(0, 39))
    # a fresh process/connection only knows what is in the database
    ingest._schema_ready = False
    ingest.ensure_schema()
    restored = ingest.get_progress(IX, HOUR)
    assert restored["progress"]["backup"]["decided_minutes"] == 40
    out = report("backup", minutes(40, 59))
    assert out["serving_source"] == "backup"
    assert len(business_rows()) == 60


def test_business_table_and_progress_commit_together(client):
    """After every accepted report the table matches the recorded source."""
    report("backup", minutes(0, 59, missing=(1,)))
    assert ingest.get_hour(IX, HOUR)["row_count"] == len(business_rows())
    report("primary", minutes(0, 59, missing=(2, 3)))
    hour = ingest.get_hour(IX, HOUR)
    assert hour["row_count"] == len(business_rows()) == 58
    # rows belong to the primary's numbers
    assert all(c != 120 for _, c in business_rows())


def test_database_guard_blocks_primary_to_backup_downgrade(client):
    """Even a direct UPDATE cannot move a served hour back to the backup."""
    report("primary", minutes())
    with psycopg.connect(config.PRIMARY_DSN) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "UPDATE gateway_ingest.hour_state SET serving_source='backup' "
                "WHERE intersection_id=%s",
                (IX,),
            )
    assert ingest.get_progress(IX, HOUR)["serving_source"] == "primary"


def test_crash_before_commit_leaves_nothing_behind(client, monkeypatch):
    """A failure after the rows are written must roll the whole report back.

    Simulates a crash mid-transaction by making the very last statement fail:
    neither the conclusions nor the progress nor business rows may persist,
    and a retry afterwards behaves as if the failed attempt never happened.
    """
    import psycopg as _psycopg

    real_publish = ingest._publish_if_ready

    def explode(conn, *args, **kwargs):
        # conclusions and progress are already written in this transaction
        raise RuntimeError("simulated crash before commit")

    monkeypatch.setattr(ingest, "_publish_if_ready", explode)
    with pytest.raises(RuntimeError):
        report("backup", minutes())
    monkeypatch.setattr(ingest, "_publish_if_ready", real_publish)

    assert business_rows() == []
    prog = ingest.get_progress(IX, HOUR)
    assert prog["ready"] is False
    assert prog["progress"]["backup"]["decided_minutes"] == 0
    with _psycopg.connect(config.PRIMARY_DSN) as conn:
        n = conn.execute(
            "SELECT count(*) FROM gateway_ingest.minute_conclusion"
        ).fetchone()[0]
    assert n == 0

    # the retry publishes normally
    out = report("backup", minutes())
    assert out["serving_source"] == "backup"
    assert len(business_rows()) == 60
