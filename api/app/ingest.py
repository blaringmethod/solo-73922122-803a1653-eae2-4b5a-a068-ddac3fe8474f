"""Dual-source (primary / backup gateway) hourly ingest and publication.

One intersection's hour is collected by two gateways. Analysts may use exactly
one source per hour, so this module decides which source serves an hour and
keeps the existing minute-count table consistent with that decision.

Rules implemented here:

* Reports are batched by intersection, gateway source and UTC whole hour. Each
  reported minute carries either a non-negative count or an explicit "missing"
  marker. A source has *reported the full hour* once all 60 minutes are decided.
* A minute conclusion is immutable per (intersection, hour, source): an
  identical retransmission is a no-op, a batch that disagrees with any already
  decided minute is rejected as a whole (nothing from it is applied).
* The first source to complete the hour gets published. Once the primary
  gateway has completed the hour the whole hour is served from the primary,
  and minutes the primary marked missing have their backup readings removed.
  Publication never moves backwards from primary to backup.
* Conclusion rows, per-source progress, the serving source/version and the
  rows in ``traffic_minute_counts`` are written in one transaction, so a
  restart resumes from the committed state and readers never observe a
  half-applied switch.
* Concurrent reports for the same intersection/hour are serialised with a
  transaction-scoped advisory lock; a committed primary publication therefore
  cannot be overwritten by a slower backup upload.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg

from . import db

log = logging.getLogger("ingest")

SCHEMA = "gateway_ingest"
BUSINESS_TABLE = "traffic_minute_counts"
SOURCES = ("primary", "backup")
MINUTES_PER_HOUR = 60

_SCHEMA_STATEMENTS = (
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    # The source database owns this table (db/init/01-init.sql). Mirrored here
    # so the API also works against a bare database outside docker.
    f"""
    CREATE TABLE IF NOT EXISTS {BUSINESS_TABLE} (
        intersection_id text        NOT NULL,
        minute_utc      timestamptz NOT NULL,
        vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
        PRIMARY KEY (intersection_id, minute_utc)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.minute_conclusion (
        intersection_id text        NOT NULL,
        hour_utc        timestamptz NOT NULL,
        source          text        NOT NULL,
        minute_utc      timestamptz NOT NULL,
        is_missing      boolean     NOT NULL,
        vehicle_count   integer,
        reported_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (intersection_id, hour_utc, source, minute_utc),
        CONSTRAINT minute_conclusion_source_chk
            CHECK (source IN ('primary', 'backup')),
        CONSTRAINT minute_conclusion_value_chk CHECK (
            (is_missing AND vehicle_count IS NULL)
            OR (NOT is_missing AND vehicle_count IS NOT NULL AND vehicle_count >= 0)
        ),
        CONSTRAINT minute_conclusion_window_chk CHECK (
            minute_utc >= hour_utc
            AND minute_utc < hour_utc + interval '1 hour'
        ),
        CONSTRAINT minute_conclusion_alignment_chk CHECK (
            hour_utc = date_trunc('hour', hour_utc, 'UTC')
            AND minute_utc = date_trunc('minute', minute_utc, 'UTC')
        )
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.source_progress (
        intersection_id text        NOT NULL,
        hour_utc        timestamptz NOT NULL,
        source          text        NOT NULL,
        decided_minutes integer     NOT NULL,
        counted_minutes integer     NOT NULL,
        missing_minutes integer     NOT NULL,
        complete        boolean     NOT NULL,
        updated_at      timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (intersection_id, hour_utc, source),
        CONSTRAINT source_progress_source_chk
            CHECK (source IN ('primary', 'backup')),
        CONSTRAINT source_progress_count_chk
            CHECK (decided_minutes BETWEEN 0 AND 60
                   AND counted_minutes + missing_minutes = decided_minutes)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.hour_state (
        intersection_id text        NOT NULL,
        hour_utc        timestamptz NOT NULL,
        serving_source  text        NOT NULL,
        version         bigint      NOT NULL CHECK (version >= 1),
        published_at    timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (intersection_id, hour_utc),
        CONSTRAINT hour_state_source_chk
            CHECK (serving_source IN ('primary', 'backup'))
    )
    """,
)

_schema_lock = threading.Lock()
_schema_ready = False

# Enforces at the storage layer the rule the API also implements: an hour that
# is served by the primary can never go back to the backup gateway. Triggers
# are created separately (and idempotently) because CREATE TRIGGER has no
# IF NOT EXISTS.
_GUARD_STATEMENTS = (
    f"""
    CREATE OR REPLACE FUNCTION {SCHEMA}.no_primary_to_backup() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF NEW.serving_source = 'backup' AND OLD.serving_source = 'primary' THEN
            RAISE EXCEPTION
                'hour %/% already served by the primary gateway',
                NEW.intersection_id, NEW.hour_utc
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END;
    $$
    """,
    f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'hour_state_no_downgrade'
              AND tgrelid = '{SCHEMA}.hour_state'::regclass
        ) THEN
            CREATE TRIGGER hour_state_no_downgrade
                BEFORE UPDATE ON {SCHEMA}.hour_state
                FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.no_primary_to_backup();
        END IF;
    EXCEPTION WHEN duplicate_object THEN
        NULL;  -- another process created it between the check and the CREATE
    END
    $$
    """,
)


class BadRequest(Exception):
    """Malformed report (maps to HTTP 400)."""


class Conflict(Exception):
    """Rejected report / not servable state (maps to HTTP 409)."""

    def __init__(self, message: str, detail: dict | None = None):
        super().__init__(message)
        self.detail = detail or {}


class NotReady(Conflict):
    """Neither gateway has completed this hour yet."""


def ensure_schema() -> None:
    """Create the ingest schema once per process (idempotent DDL).

    Concurrent API processes can race ``CREATE ... IF NOT EXISTS`` (Postgres
    may raise duplicate_table / unique_violation, or fail the trigger check).
    We serialise this bootstrap across processes with the same bounded
    try-lock used for hour writes, and retry the batch on a duplicate error.
    """
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        retryable = (
            psycopg.errors.DuplicateTable,
            psycopg.errors.UniqueViolation,
            psycopg.errors.DuplicateObject,
        )
        for attempt in range(3):
            try:
                with db.primary_tx() as conn:
                    _try_lock_once(conn, SCHEMA, wait_sec=60)
                    for statement in _SCHEMA_STATEMENTS:
                        conn.execute(statement)
                    for statement in _GUARD_STATEMENTS:
                        conn.execute(statement)
                _schema_ready = True
                log.info("dual-source ingest schema ready")
                return
            except retryable:
                if attempt == 2:
                    raise
                log.info("ingest schema was created concurrently; retrying")


# --------------------------------------------------------------------------
# time helpers
# --------------------------------------------------------------------------

def _as_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BadRequest(f"{field} must include a UTC offset (e.g. ...Z)")
    return value.astimezone(timezone.utc)


def normalize_hour(value: datetime) -> datetime:
    hour = _as_utc(value, field="hour_utc")
    if hour.minute or hour.second or hour.microsecond:
        raise BadRequest(
            "hour_utc must be a whole UTC hour (minutes/seconds must be 00)"
        )
    return hour.replace(tzinfo=timezone.utc)


def _normalize_minute(value: datetime, hour: datetime) -> datetime:
    minute = _as_utc(value, field="minute_utc")
    if minute.second or minute.microsecond:
        raise BadRequest(
            f"minute_utc {minute.isoformat()} is not aligned to a whole minute"
        )
    if not (hour <= minute < hour + timedelta(hours=1)):
        raise BadRequest(
            f"minute_utc {minute.isoformat()} is outside hour {hour.isoformat()}"
        )
    return minute.replace(tzinfo=timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _lock_key(intersection_id: str, hour: datetime) -> str:
    return f"{intersection_id}|{iso(hour)}"


# How long a report may wait for another report of the same intersection/hour.
# A blocking `pg_advisory_xact_lock` is the obvious choice, but this bounded
# spin also survives environments where a backend blocked in the kernel is
# never woken (observed in a restricted sandbox). Both give the same guarantee:
# one writer per intersection/hour at a time, held until commit.
LOCK_WAIT_SEC = 20.0
LOCK_POLL_SEC = 0.05


def _try_lock_once(conn, key: str, *, wait_sec: float | None = None) -> None:
    """Take a transaction-scoped advisory lock, polling instead of blocking."""
    if wait_sec is None:
        wait_sec = LOCK_WAIT_SEC
    deadline = time.monotonic() + wait_sec
    while True:
        locked = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
            (key,),
        ).fetchone()["pg_try_advisory_xact_lock"]
        if locked:
            return
        if time.monotonic() >= deadline:
            raise Conflict(f"{key} is locked by a concurrent writer; retry shortly")
        time.sleep(LOCK_POLL_SEC)


def _acquire_hour_lock(conn, intersection_id: str, hour: datetime) -> None:
    _try_lock_once(conn, _lock_key(intersection_id, hour))


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def report(
    intersection_id: str,
    source: str,
    hour_utc: datetime,
    minutes: list[dict],
) -> dict:
    """Apply one gateway batch and publish if it completes the hour.

    ``minutes`` items are dicts with ``minute`` (aware datetime),
    ``is_missing`` (bool) and ``vehicle_count`` (int or None).
    """
    ensure_schema()
    if not isinstance(intersection_id, str) or not intersection_id:
        raise BadRequest("intersection_id must be a non-empty string")
    if source not in SOURCES:
        raise BadRequest(f"source must be one of {SOURCES}")
    hour = normalize_hour(hour_utc)

    decisions: dict[datetime, tuple[bool, int | None]] = {}
    for item in minutes:
        minute = _normalize_minute(item["minute"], hour)
        if minute in decisions:
            raise BadRequest(f"minute {iso(minute)} appears more than once in the batch")
        is_missing = bool(item["is_missing"])
        count = item["vehicle_count"]
        if is_missing:
            if count is not None:
                raise BadRequest(
                    f"minute {iso(minute)} is marked missing but also has a count"
                )
            decisions[minute] = (True, None)
        else:
            if count is None or count < 0:
                raise BadRequest(
                    f"minute {iso(minute)} needs a non-negative vehicle_count "
                    "or an explicit missing marker"
                )
            decisions[minute] = (False, int(count))
    if not decisions:
        raise BadRequest("a batch must contain at least one minute")
    if len(decisions) > MINUTES_PER_HOUR:
        raise BadRequest("a batch cannot cover more than 60 minutes")

    with db.primary_tx() as conn:
        # Serialise every writer of this intersection/hour. Held until commit,
        # so a primary publication can never be raced by a backup upload.
        _acquire_hour_lock(conn, intersection_id, hour)

        existing = {
            row["minute_utc"]: (row["is_missing"], row["vehicle_count"])
            for row in conn.execute(
                f"SELECT minute_utc, is_missing, vehicle_count "
                f"FROM {SCHEMA}.minute_conclusion "
                f"WHERE intersection_id = %s AND hour_utc = %s AND source = %s",
                (intersection_id, hour, source),
            )
        }

        conflicts = []
        new_decisions = []
        for minute, (is_missing, count) in sorted(decisions.items()):
            prior = existing.get(minute)
            if prior is None:
                new_decisions.append((minute, is_missing, count))
            elif (prior[0], prior[1]) != (is_missing, count):
                conflicts.append(
                    {
                        "minute_utc": iso(minute),
                        "stored_missing": prior[0],
                        "stored_vehicle_count": prior[1],
                        "submitted_missing": is_missing,
                        "submitted_vehicle_count": count,
                    }
                )
        if conflicts:
            # Nothing from this batch is applied: the transaction rolls back,
            # including any would-be new minutes alongside the conflicts.
            raise Conflict(
                f"batch rejected: {len(conflicts)} minute(s) disagree with "
                "already reported conclusions for this source",
                detail={"conflicts": conflicts},
            )

        if new_decisions:
            with conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {SCHEMA}.minute_conclusion "
                    f"(intersection_id, hour_utc, source, minute_utc, "
                    f" is_missing, vehicle_count) "
                    f"VALUES (%s, %s, %s, %s, %s, %s)",
                    [
                        (intersection_id, hour, source, minute, is_missing, count)
                        for minute, is_missing, count in new_decisions
                    ],
                )

        progress = _refresh_progress(conn, intersection_id, hour)

        published, serving_source, version = _publish_if_ready(
            conn, intersection_id, hour, progress
        )

        return {
            "intersection_id": intersection_id,
            "hour_utc": iso(hour),
            "source": source,
            "new_minutes": len(new_decisions),
            "unchanged_minutes": len(decisions) - len(new_decisions),
            "changed": bool(new_decisions) or published,
            "published": published,
            "ready": serving_source is not None,
            "serving_source": serving_source,
            "version": version,
            "progress": progress,
        }


def _refresh_progress(conn, intersection_id: str, hour: datetime) -> dict:
    """Recompute both sources' progress from conclusions in this transaction."""
    progress = {
        source: {
            "source": source,
            "decided_minutes": 0,
            "counted_minutes": 0,
            "missing_minutes": 0,
            "complete": False,
        }
        for source in SOURCES
    }
    rows = conn.execute(
        f"SELECT source, "
        f"       count(*)::int AS decided_minutes, "
        f"       count(*) FILTER (WHERE NOT is_missing)::int AS counted_minutes, "
        f"       count(*) FILTER (WHERE is_missing)::int AS missing_minutes "
        f"FROM {SCHEMA}.minute_conclusion "
        f"WHERE intersection_id = %s AND hour_utc = %s "
        f"GROUP BY source",
        (intersection_id, hour),
    ).fetchall()
    for row in rows:
        source = row["source"]
        progress[source] = {
            "source": source,
            "decided_minutes": row["decided_minutes"],
            "counted_minutes": row["counted_minutes"],
            "missing_minutes": row["missing_minutes"],
            "complete": row["decided_minutes"] >= MINUTES_PER_HOUR,
        }
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {SCHEMA}.source_progress "
            f"(intersection_id, hour_utc, source, decided_minutes, "
            f" counted_minutes, missing_minutes, complete) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s) "
            f"ON CONFLICT (intersection_id, hour_utc, source) DO UPDATE SET "
            f"  decided_minutes = EXCLUDED.decided_minutes, "
            f"  counted_minutes = EXCLUDED.counted_minutes, "
            f"  missing_minutes = EXCLUDED.missing_minutes, "
            f"  complete = EXCLUDED.complete, "
            f"  updated_at = now()",
            [
                (
                    intersection_id,
                    hour,
                    item["source"],
                    item["decided_minutes"],
                    item["counted_minutes"],
                    item["missing_minutes"],
                    item["complete"],
                )
                for item in progress.values()
                if item["decided_minutes"] > 0
            ],
        )
    return progress


def _publish_if_ready(conn, intersection_id, hour, progress):
    """Pick the serving source and reconcile the business rows atomically.

    Returns ``(published_now, serving_source, version)``.
    """
    state = conn.execute(
        f"SELECT serving_source, version FROM {SCHEMA}.hour_state "
        f"WHERE intersection_id = %s AND hour_utc = %s",
        (intersection_id, hour),
    ).fetchone()
    serving = state["serving_source"] if state else None

    if progress["primary"]["complete"]:
        target = "primary"
    elif progress["backup"]["complete"] and serving != "primary":
        target = "backup"
    else:
        target = None

    if target is None:
        # Nothing may be published yet, or the primary already owns the hour
        # and a late backup completion must not pull it back.
        return False, serving, state["version"] if state else None

    desired = {
        row["minute_utc"]: row["vehicle_count"]
        for row in conn.execute(
            f"SELECT minute_utc, vehicle_count FROM {SCHEMA}.minute_conclusion "
            f"WHERE intersection_id = %s AND hour_utc = %s AND source = %s "
            f"AND NOT is_missing",
            (intersection_id, hour, target),
        )
    }
    current = {
        row["minute_utc"]: row["vehicle_count"]
        for row in conn.execute(
            f"SELECT minute_utc, vehicle_count FROM {BUSINESS_TABLE} "
            f"WHERE intersection_id = %s AND minute_utc >= %s AND minute_utc < %s",
            (intersection_id, hour, hour + timedelta(hours=1)),
        )
    }

    source_changed = serving != target
    if not source_changed and current == desired:
        return False, serving, state["version"]

    # Replace the whole hour so no reading from the other gateway survives,
    # in particular minutes the newly serving source marked as missing.
    conn.execute(
        f"DELETE FROM {BUSINESS_TABLE} "
        f"WHERE intersection_id = %s AND minute_utc >= %s AND minute_utc < %s",
        (intersection_id, hour, hour + timedelta(hours=1)),
    )
    if desired:
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {BUSINESS_TABLE} "
                f"(intersection_id, minute_utc, vehicle_count) VALUES (%s, %s, %s)",
                [
                    (intersection_id, minute, count)
                    for minute, count in sorted(desired.items())
                ],
            )

    if state is None:
        version = 1
        conn.execute(
            f"INSERT INTO {SCHEMA}.hour_state "
            f"(intersection_id, hour_utc, serving_source, version) "
            f"VALUES (%s, %s, %s, %s)",
            (intersection_id, hour, target, version),
        )
    else:
        version = state["version"] + 1
        conn.execute(
            f"UPDATE {SCHEMA}.hour_state SET serving_source = %s, version = %s, "
            f"published_at = now() "
            f"WHERE intersection_id = %s AND hour_utc = %s",
            (target, version, intersection_id, hour),
        )
    log.info(
        "published %s/%s from %s as version %s (%d rows)",
        intersection_id, iso(hour), target, version, len(desired),
    )
    return True, target, version


# --------------------------------------------------------------------------
# querying
# --------------------------------------------------------------------------

def _read_state(conn, intersection_id: str, hour: datetime):
    state = conn.execute(
        f"SELECT serving_source, version, published_at FROM {SCHEMA}.hour_state "
        f"WHERE intersection_id = %s AND hour_utc = %s",
        (intersection_id, hour),
    ).fetchone()
    progress = {
        source: {
            "source": source,
            "decided_minutes": 0,
            "counted_minutes": 0,
            "missing_minutes": 0,
            "complete": False,
        }
        for source in SOURCES
    }
    for row in conn.execute(
        f"SELECT source, decided_minutes, counted_minutes, missing_minutes, complete "
        f"FROM {SCHEMA}.source_progress "
        f"WHERE intersection_id = %s AND hour_utc = %s",
        (intersection_id, hour),
    ):
        progress[row["source"]] = {
            "source": row["source"],
            "decided_minutes": row["decided_minutes"],
            "counted_minutes": row["counted_minutes"],
            "missing_minutes": row["missing_minutes"],
            "complete": row["complete"],
        }
    return state, progress


def get_hour(intersection_id: str, hour_utc: datetime) -> dict:
    """Return the serving source and its detail from one consistent snapshot."""
    ensure_schema()
    hour = normalize_hour(hour_utc)
    # REPEATABLE READ: the serving source/version and the rows it describes
    # are read from the same snapshot, never from two different publications.
    with db.primary_tx(repeatable_read=True, read_only=True) as conn:
        state, progress = _read_state(conn, intersection_id, hour)
        if state is None:
            raise NotReady(
                f"{intersection_id} {iso(hour)} is not ready: neither gateway "
                "has reported all 60 minutes yet",
                detail={
                    "intersection_id": intersection_id,
                    "hour_utc": iso(hour),
                    "ready": False,
                    "serving_source": None,
                    "progress": progress,
                },
            )
        rows = conn.execute(
            f"SELECT minute_utc, vehicle_count FROM {BUSINESS_TABLE} "
            f"WHERE intersection_id = %s AND minute_utc >= %s AND minute_utc < %s "
            f"ORDER BY minute_utc",
            (intersection_id, hour, hour + timedelta(hours=1)),
        ).fetchall()
        missing = conn.execute(
            f"SELECT minute_utc FROM {SCHEMA}.minute_conclusion "
            f"WHERE intersection_id = %s AND hour_utc = %s AND source = %s "
            f"AND is_missing ORDER BY minute_utc",
            (intersection_id, hour, state["serving_source"]),
        ).fetchall()
        return {
            "intersection_id": intersection_id,
            "hour_utc": iso(hour),
            "ready": True,
            "serving_source": state["serving_source"],
            "version": state["version"],
            "published_at": iso(state["published_at"]),
            "row_count": len(rows),
            "rows": [
                {"minute_utc": iso(row["minute_utc"]), "vehicle_count": row["vehicle_count"]}
                for row in rows
            ],
            "missing_minutes": [iso(row["minute_utc"]) for row in missing],
            "progress": progress,
        }


def get_progress(intersection_id: str, hour_utc: datetime) -> dict:
    """Report per-source progress without requiring a published hour."""
    ensure_schema()
    hour = normalize_hour(hour_utc)
    with db.primary_tx(repeatable_read=True, read_only=True) as conn:
        state, progress = _read_state(conn, intersection_id, hour)
        return {
            "intersection_id": intersection_id,
            "hour_utc": iso(hour),
            "ready": state is not None,
            "serving_source": state["serving_source"] if state else None,
            "version": state["version"] if state else None,
            "published_at": iso(state["published_at"]) if state else None,
            "progress": progress,
        }
