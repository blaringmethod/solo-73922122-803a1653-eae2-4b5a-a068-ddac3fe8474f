-- Business table: intersection id, UTC minute and vehicle count. This is the
-- table the PITR demo deletes from and recovers; dual-source ingest publishes
-- into it.
CREATE TABLE IF NOT EXISTS traffic_minute_counts (
    intersection_id text        NOT NULL,
    minute_utc      timestamptz NOT NULL,
    vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
    PRIMARY KEY (intersection_id, minute_utc)
);

-- Dual-source hourly ingest bookkeeping. It lives in the same database as the
-- business table on purpose: a gateway's conclusions, both sources' progress,
-- the serving source/version and the published business rows must commit or
-- roll back together, which a separate SQLite ledger could not guarantee.
-- The API also creates this schema at startup (see api/app/ingest.py), so the
-- definitions there are the authoritative copy; this file makes a fresh
-- database usable before the API starts.
CREATE SCHEMA IF NOT EXISTS gateway_ingest;

-- Immutable per-source minute conclusions: a count, or an explicit missing
-- marker (vehicle_count IS NULL).
CREATE TABLE IF NOT EXISTS gateway_ingest.minute_conclusion (
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
);

-- Per-source upload progress; "complete" means all 60 minutes are decided
-- (counted or explicitly missing).
CREATE TABLE IF NOT EXISTS gateway_ingest.source_progress (
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
);

-- The source currently serving an intersection/hour, and a monotonically
-- increasing publication version.
CREATE TABLE IF NOT EXISTS gateway_ingest.hour_state (
    intersection_id text        NOT NULL,
    hour_utc        timestamptz NOT NULL,
    serving_source  text        NOT NULL,
    version         bigint      NOT NULL CHECK (version >= 1),
    published_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (intersection_id, hour_utc),
    CONSTRAINT hour_state_source_chk
        CHECK (serving_source IN ('primary', 'backup'))
);

-- Storage-level guard for the core rule: a served hour may move from the
-- backup gateway to the primary gateway, never the other way.
CREATE OR REPLACE FUNCTION gateway_ingest.no_primary_to_backup() RETURNS trigger
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
$$;

DROP TRIGGER IF EXISTS hour_state_no_downgrade ON gateway_ingest.hour_state;
CREATE TRIGGER hour_state_no_downgrade
    BEFORE UPDATE ON gateway_ingest.hour_state
    FOR EACH ROW EXECUTE FUNCTION gateway_ingest.no_primary_to_backup();
