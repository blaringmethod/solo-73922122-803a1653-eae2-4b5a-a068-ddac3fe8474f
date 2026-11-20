"""Traffic data-warehouse PITR recovery API.

Scenario: an operator accidentally deleted intersection minute counts from
the source database. The primary keeps collecting; this service rebuilds an
isolated read-only replica from the startup physical base backup + archived
WAL, up to a user-created named restore point, so the deleted rows can be
queried and back-filled.

Demo entry point: run the stack, then open /docs (Swagger UI).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field, model_validator

from . import backup, config, db, ingest, lsn, recovery, state

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("api")

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


def _readiness_loop():
    while True:
        try:
            recovery.refresh_restore_points()
        except Exception:
            log.exception("readiness refresh failed")
        time.sleep(2)


def _monitor_loop():
    while True:
        try:
            recovery.monitor_current()
        except Exception:
            log.exception("replica monitor failed")
        time.sleep(3)


@asynccontextmanager
async def lifespan(_app):
    os.makedirs(config.RECOVERY_ROOT, exist_ok=True)
    os.makedirs(os.path.dirname(config.STATE_DB), exist_ok=True)
    state.init()
    recovery.cleanup_on_startup()
    try:
        ingest.ensure_schema()
    except Exception:  # noqa: BLE001 - endpoints retry lazily
        log.exception("could not create the dual-source ingest schema yet")
    # physical base backup is established at startup, in the background
    threading.Thread(target=backup.run_base_backup, daemon=True, name="base-backup").start()
    threading.Thread(target=_readiness_loop, daemon=True, name="readiness").start()
    threading.Thread(target=_monitor_loop, daemon=True, name="monitor").start()
    yield


app = FastAPI(
    title="Traffic PITR Recovery + Dual-Source Ingest API",
    version="1.1.0",
    description=(
        "Two capabilities over the traffic minute counts. "
        "(A) Dual-source hourly ingest: gateways report per intersection, "
        "source and UTC hour; the first source to decide all 60 minutes is "
        "published, the primary takes the hour over once complete, and "
        "primary-missing minutes drop the backup readings. "
        "(B) Physical point-in-time recovery for the traffic_minute_counts "
        "table: POST /demo/generate, POST /restore-points, POST /demo/delete, "
        "POST /recoveries, GET /replica/traffic. "
        "Demo entry point: /docs."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


# --------------------------------------------------------------------------
# system
# --------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    try:
        with db.primary() as conn:
            conn.execute("SELECT 1")
        primary_ok = True
    except Exception:
        primary_ok = False
    return {"status": "ok" if primary_ok else "degraded", "primary_reachable": primary_ok}


@app.get("/backup", tags=["system"])
def backup_status():
    """Status of the physical base backup taken at startup."""
    return {
        "status": state.kv_get("backup_status", "unknown"),
        "end_lsn": state.kv_get("backup_end_lsn"),
        "completed_at": state.kv_get("backup_completed_at"),
        "error": state.kv_get("backup_error") or None,
        "backup_dir": config.BACKUP_DIR,
    }


# --------------------------------------------------------------------------
# restore points
# --------------------------------------------------------------------------

@app.post("/restore-points", status_code=201, tags=["restore-points"])
def create_restore_point(name: str = Query(..., pattern=NAME_PATTERN)):
    """Create a named restore point on the primary (pg_create_restore_point).

    The point becomes selectable for recovery only after every WAL segment
    between the base backup and the point has shown up in the archive.
    """
    if state.kv_get("backup_status") != "ready":
        raise HTTPException(
            409, "base backup not ready yet; restore points must be created after it"
        )
    if state.one("SELECT id FROM restore_points WHERE name=?", (name,)):
        raise HTTPException(409, f"restore point {name!r} already exists")
    with db.primary() as conn:
        point_lsn = conn.execute(
            "SELECT pg_create_restore_point(%s)::text", (name,)
        ).fetchone()[0]
        # force the segment holding the point to be archived promptly
        conn.execute("SELECT pg_switch_wal()")
    wal_file = lsn.wal_file(lsn.parse(point_lsn))
    point_id = state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file) VALUES (?, ?, ?)",
        (name, point_lsn, wal_file),
    )
    recovery.refresh_restore_points()
    return state.one("SELECT * FROM restore_points WHERE id=?", (point_id,))


@app.get("/restore-points", tags=["restore-points"])
def list_restore_points():
    recovery.refresh_restore_points()
    return {"items": state.q("SELECT * FROM restore_points ORDER BY id")}


# --------------------------------------------------------------------------
# recoveries
# --------------------------------------------------------------------------

class RecoveryRequest(BaseModel):
    restore_point: str = Field(..., pattern=NAME_PATTERN)


@app.post("/recoveries", status_code=202, tags=["recoveries"])
def start_recovery(req: RecoveryRequest):
    """Start a recovery to a named restore point (only one may be in flight).

    Rebuilds a fresh isolated replica from the physical base backup; any
    previously served replica is retired first, so points are never mixed.
    """
    try:
        return recovery.start_recovery(req.restore_point)
    except recovery.NotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/recoveries", tags=["recoveries"])
def list_recoveries():
    return {"items": state.q("SELECT * FROM recoveries ORDER BY id DESC")}


@app.get("/recoveries/current", tags=["recoveries"])
def current_recovery():
    try:
        return recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/recoveries/{rec_id}", tags=["recoveries"])
def get_recovery(rec_id: int):
    rec = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    if not rec:
        raise HTTPException(404, f"recovery {rec_id} not found")
    return rec


# --------------------------------------------------------------------------
# querying the recovered replica
# --------------------------------------------------------------------------

@app.get("/replica/traffic", tags=["replica"])
def replica_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Query the recovered read-only replica (409 while none is servable)."""
    try:
        rec = recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc
    try:
        with db.replica() as conn:
            where = "WHERE intersection_id = %(id)s" if intersection_id else ""
            rows = conn.execute(
                f"SELECT intersection_id, minute_utc, vehicle_count "
                f"FROM {config.TRAFFIC_TABLE} {where} "
                f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
                {"id": intersection_id, "lim": limit},
            ).fetchall()
            total = conn.execute(
                f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
            ).fetchone()["n"]
            meta = conn.execute(
                "SELECT pg_is_in_recovery() AS in_recovery, "
                "pg_last_wal_replay_lsn()::text AS replay_lsn"
            ).fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, f"replica query failed: {exc}") from exc
    return {
        "recovery": {
            "id": rec["id"],
            "restore_point": rec["restore_point"],
            "target_lsn": rec["target_lsn"],
            "replay_lsn": meta["replay_lsn"],
            "in_recovery_read_only": meta["in_recovery"],
        },
        "total_rows": total,
        "rows": rows,
    }


# --------------------------------------------------------------------------
# dual-source hourly ingest
# --------------------------------------------------------------------------

class MinuteReport(BaseModel):
    """One minute's conclusion from a gateway: a count or an explicit gap."""

    minute_utc: datetime
    vehicle_count: int | None = Field(default=None, ge=0)
    missing: bool = False

    @model_validator(mode="after")
    def _one_conclusion(self):
        if self.missing:
            if self.vehicle_count is not None:
                raise ValueError("a missing minute cannot carry a vehicle_count")
        elif self.vehicle_count is None:
            raise ValueError(
                "each minute needs vehicle_count >= 0, or missing=true"
            )
        return self


class IngestReport(BaseModel):
    """A batch of minute conclusions for one intersection, source and hour."""

    intersection_id: str = Field(..., min_length=1, max_length=128)
    source: str = Field(..., pattern=r"^(primary|backup)$")
    hour_utc: datetime
    minutes: list[MinuteReport] = Field(..., min_length=1, max_length=60)


def _conflict(exc: ingest.Conflict) -> HTTPException:
    return HTTPException(409, {"message": str(exc), **exc.detail})


@app.post(
    "/ingest/reports",
    tags=["dual-source ingest"],
    responses={409: {"description": "batch rejected or a concurrent report holds the hour"}},
)
def ingest_report(report: IngestReport):
    """Report minute conclusions for one intersection/source/UTC hour.

    Idempotent for an identical retransmission; a batch that disagrees with
    already recorded minutes for that source is rejected as a whole (409).
    The first source to decide all 60 minutes is published; once the primary
    has done so the hour is served from the primary, whose missing minutes
    drop the backup readings. Progress, serving source and
    ``traffic_minute_counts`` commit in one transaction.
    """
    try:
        return ingest.report(
            report.intersection_id,
            report.source,
            report.hour_utc,
            [
                {
                    "minute": item.minute_utc,
                    "is_missing": item.missing,
                    "vehicle_count": item.vehicle_count,
                }
                for item in report.minutes
            ],
        )
    except ingest.BadRequest as exc:
        raise HTTPException(400, str(exc)) from exc
    except ingest.Conflict as exc:
        raise _conflict(exc) from exc


@app.get("/ingest/hours", tags=["dual-source ingest"])
def ingest_hour(
    intersection_id: str = Query(..., min_length=1, max_length=128),
    hour_utc: datetime = Query(..., description="whole UTC hour, e.g. 2026-11-20T08:00:00Z"),
):
    """Query one intersection/hour: serving source plus that version's detail.

    Returns 409 (with per-source progress) while neither gateway has reported
    the full hour. The serving source, version and rows come from a single
    repeatable-read snapshot, so they always belong to the same publication.
    """
    try:
        return ingest.get_hour(intersection_id, hour_utc)
    except ingest.BadRequest as exc:
        raise HTTPException(400, str(exc)) from exc
    except ingest.NotReady as exc:
        raise HTTPException(
            409, {"message": str(exc), "not_ready": True, **exc.detail}
        ) from exc


@app.get("/ingest/progress", tags=["dual-source ingest"])
def ingest_progress(
    intersection_id: str = Query(..., min_length=1, max_length=128),
    hour_utc: datetime = Query(..., description="whole UTC hour, e.g. 2026-11-20T08:00:00Z"),
):
    """Per-source upload progress for one intersection/hour (no readiness gate)."""
    try:
        return ingest.get_progress(intersection_id, hour_utc)
    except ingest.BadRequest as exc:
        raise HTTPException(400, str(exc)) from exc


# --------------------------------------------------------------------------
# demo: simulated collection + accidental deletion on the primary
# --------------------------------------------------------------------------

@app.post("/demo/generate", tags=["demo"])
def demo_generate(
    intersections: int = Query(3, ge=1, le=100),
    minutes: int = Query(20, ge=1, le=10000),
    prefix: str = Query("IX", pattern=r"^[A-Za-z0-9_-]{1,16}$"),
):
    """Simulate collection: upsert intersection/minute counts on the primary."""
    sql = f"""
        INSERT INTO {config.TRAFFIC_TABLE} (intersection_id, minute_utc, vehicle_count)
        SELECT %(prefix)s || '-' || i,
               date_trunc('minute', now()) - (m || ' minutes')::interval,
               (random() * 90 + 10)::int
        FROM generate_series(1, %(n)s) AS i
        CROSS JOIN generate_series(1, %(m)s) AS m
        ON CONFLICT (intersection_id, minute_utc)
        DO UPDATE SET vehicle_count = EXCLUDED.vehicle_count
        RETURNING intersection_id
    """
    with db.primary() as conn:
        inserted = len(
            conn.execute(
                sql, {"prefix": prefix, "n": intersections, "m": minutes}
            ).fetchall()
        )
    return {
        "upserted_rows": inserted,
        "intersections": [f"{prefix}-{i}" for i in range(1, intersections + 1)],
        "minutes_covered": minutes,
    }


@app.post("/demo/delete", tags=["demo"])
def demo_delete(minutes: int = Query(10, ge=1, le=100000)):
    """Simulate the accident: delete the latest `minutes` minutes of data."""
    sql = f"""
        DELETE FROM {config.TRAFFIC_TABLE}
        WHERE minute_utc > COALESCE(
                  (SELECT max(minute_utc) FROM {config.TRAFFIC_TABLE}),
                  date_trunc('minute', now())
              ) - (%(m)s || ' minutes')::interval
        RETURNING intersection_id
    """
    with db.primary() as conn:
        deleted = len(conn.execute(sql, {"m": minutes}).fetchall())
    return {"deleted_rows": deleted, "note": "simulated accidental deletion on the primary"}


@app.get("/primary/traffic", tags=["demo"])
def primary_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Current state of the primary (which keeps collecting during recovery)."""
    where = "WHERE intersection_id = %(id)s" if intersection_id else ""
    with db.primary(row_factory=dict_row) as conn:
        rows = conn.execute(
            f"SELECT intersection_id, minute_utc, vehicle_count "
            f"FROM {config.TRAFFIC_TABLE} {where} "
            f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
            {"id": intersection_id, "lim": limit},
        ).fetchall()
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
    return {"total_rows": total, "rows": rows}


@app.get("/demo/overview", tags=["demo"])
def demo_overview():
    """One-shot picture: primary data, backup, restore points, recoveries."""
    with db.primary(row_factory=dict_row) as conn:
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
        per_ix = conn.execute(
            f"SELECT intersection_id, count(*) AS rows, "
            f"min(minute_utc) AS first_minute, max(minute_utc) AS last_minute "
            f"FROM {config.TRAFFIC_TABLE} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return {
        "primary": {"total_rows": total, "per_intersection": per_ix},
        "backup": backup_status(),
        "restore_points": state.q("SELECT * FROM restore_points ORDER BY id"),
        "recoveries": state.q("SELECT * FROM recoveries ORDER BY id DESC"),
        "current_recovery_id": state.kv_get("current_recovery_id"),
    }


@app.post("/demo/dual-source/generate", tags=["demo"])
def demo_dual_source_generate(
    intersection_id: str = Query("IX-DEMO", min_length=1, max_length=128),
    source: str = Query(..., pattern=r"^(primary|backup)$"),
    hour_utc: datetime = Query(..., description="whole UTC hour, e.g. 2026-11-20T08:00:00Z"),
    start_minute: int = Query(0, ge=0, le=59),
    end_minute: int = Query(59, ge=0, le=59),
    missing_start: int | None = Query(None, ge=0, le=59),
    missing_end: int | None = Query(None, ge=0, le=59),
    base_count: int = Query(20, ge=0),
):
    """Feed one gateway's conclusions through the real ingest path.

    Generates an inclusive minute range for one source and posts it via the
    same logic as ``POST /ingest/reports``. Counts are deterministic
    (``base_count + offset`` for the primary, ``base_count + 100 + offset``
    for the backup) so the same call twice is a true retransmission and the
    source switch is visible in the returned rows. ``missing_start``..
    ``missing_end`` marks that inclusive range as explicitly unreported.
    """
    if start_minute > end_minute:
        raise HTTPException(400, "start_minute must not be greater than end_minute")
    if (missing_start is None) != (missing_end is None):
        raise HTTPException(
            400, "missing_start and missing_end must be given together"
        )
    if (
        missing_start is not None
        and missing_end is not None
        and missing_start > missing_end
    ):
        raise HTTPException(400, "missing_start must not be greater than missing_end")

    minutes = []
    for offset in range(start_minute, end_minute + 1):
        minute = hour_utc + timedelta(minutes=offset)
        is_missing = (
            missing_start is not None
            and missing_end is not None
            and missing_start <= offset <= missing_end
        )
        count = None if is_missing else base_count + (0 if source == "primary" else 100) + offset
        minutes.append(
            {"minute": minute, "is_missing": is_missing, "vehicle_count": count}
        )
    try:
        result = ingest.report(intersection_id, source, hour_utc, minutes)
    except ingest.BadRequest as exc:
        raise HTTPException(400, str(exc)) from exc
    except ingest.Conflict as exc:
        raise _conflict(exc) from exc
    return {"submitted_minutes": len(minutes), **result}


@app.post("/demo/reset", tags=["demo"])
def demo_reset(confirm: bool = Query(False)):
    """Demo only: empty the business table and dual-source bookkeeping.

    The PITR demo asserts absolute row counts, so run this (or a fresh stack)
    before mixing the two demos. Requires ``confirm=true``; refuses while a
    recovery is in flight. Restore points/recovery bookkeeping is untouched.
    """
    if not confirm:
        raise HTTPException(400, "pass confirm=true to wipe business and ingest data")
    if state.one("SELECT id FROM recoveries WHERE status='running'"):
        raise HTTPException(409, "a recovery is in flight; reset refused")
    ingest.ensure_schema()
    with db.primary_tx() as conn:
        conn.execute(f"DELETE FROM {config.TRAFFIC_TABLE}")
        conn.execute("DELETE FROM gateway_ingest.minute_conclusion")
        conn.execute("DELETE FROM gateway_ingest.source_progress")
        conn.execute("DELETE FROM gateway_ingest.hour_state")
    return {"reset": True}
