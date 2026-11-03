-- All timestamps are Unix milliseconds, UTC, from the server's clock.

CREATE TABLE monitors (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    target      TEXT NOT NULL,
    interval_ms INTEGER NOT NULL,
    regions     TEXT NOT NULL,              -- JSON array
    paused      INTEGER NOT NULL DEFAULT 0,
    -- Removed from vigil.yaml. Rows are kept so history and past incidents
    -- stay readable; re-adding the same id resumes the history.
    archived    INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- The consensus state machine's memory, one JSON document per monitor.
CREATE TABLE monitor_state (
    monitor_id TEXT PRIMARY KEY REFERENCES monitors (id),
    state      TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE results (
    -- The job ID. Probes deliver results at least once; this key turns a
    -- redelivery into a no-op instead of a second failure in a streak.
    id          TEXT NOT NULL UNIQUE,
    monitor_id  TEXT NOT NULL,
    region      TEXT NOT NULL,
    probe_id    TEXT NOT NULL,
    slot        INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    ok          INTEGER NOT NULL,
    latency_ms  REAL NOT NULL,
    status_code INTEGER NOT NULL DEFAULT 0,
    error       TEXT NOT NULL DEFAULT '',
    warning     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX results_by_monitor ON results (monitor_id, slot);
CREATE INDEX results_by_slot ON results (slot);

-- Hourly and daily aggregates. The sketch holds the latency distribution of
-- successful checks so percentiles survive aggregation.
CREATE TABLE rollups (
    resolution TEXT NOT NULL CHECK (resolution IN ('hour', 'day')),
    monitor_id TEXT NOT NULL,
    region     TEXT NOT NULL,
    bucket     INTEGER NOT NULL,
    total      INTEGER NOT NULL,
    ok         INTEGER NOT NULL,
    sketch     BLOB NOT NULL,
    PRIMARY KEY (resolution, monitor_id, region, bucket)
) WITHOUT ROWID;
CREATE INDEX rollups_by_bucket ON rollups (resolution, bucket);

CREATE TABLE incidents (
    id          INTEGER PRIMARY KEY,
    monitor_id  TEXT NOT NULL,
    opened_at   INTEGER NOT NULL,
    resolved_at INTEGER,
    impact      TEXT NOT NULL,
    regions     TEXT NOT NULL,   -- JSON array of the regions failing most recently
    voting      INTEGER NOT NULL,
    cause       TEXT NOT NULL DEFAULT '',
    flapping    INTEGER NOT NULL DEFAULT 0,
    -- The state machine names incidents by (monitor, open time), so it can
    -- stay free of database IDs.
    UNIQUE (monitor_id, opened_at)
);
CREATE INDEX incidents_open ON incidents (monitor_id) WHERE resolved_at IS NULL;
CREATE INDEX incidents_by_time ON incidents (opened_at);

CREATE TABLE incident_updates (
    id          INTEGER PRIMARY KEY,
    incident_id INTEGER NOT NULL REFERENCES incidents (id) ON DELETE CASCADE,
    at          INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('investigating', 'identified', 'monitoring', 'update', 'resolved')),
    body        TEXT NOT NULL,
    author      TEXT NOT NULL
);
CREATE INDEX incident_updates_by_incident ON incident_updates (incident_id, at);

-- Transactional outbox. Rows are written in the same transaction as the
-- state change that caused them; a dispatcher delivers them afterwards.
-- The unique key is what makes "exactly one opened alert per incident per
-- channel" hold across crashes, retries, and duplicate results.
CREATE TABLE notifications (
    id              INTEGER PRIMARY KEY,
    notifier        TEXT NOT NULL,
    dedup_key       TEXT NOT NULL,
    incident_id     INTEGER REFERENCES incidents (id),
    monitor_id      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    message         TEXT NOT NULL,   -- JSON snapshot rendered at send time
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    last_error      TEXT NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL,
    sent_at         INTEGER,
    UNIQUE (notifier, dedup_key)
);
CREATE INDEX notifications_due ON notifications (next_attempt_at) WHERE status = 'pending';

CREATE TABLE probes (
    id               TEXT PRIMARY KEY,
    region           TEXT NOT NULL,
    version          TEXT NOT NULL,
    embedded         INTEGER NOT NULL DEFAULT 0,
    remote_addr      TEXT NOT NULL DEFAULT '',
    first_seen_at    INTEGER NOT NULL,
    last_seen_at     INTEGER NOT NULL,
    clock_skew_ms    INTEGER NOT NULL DEFAULT 0,
    checks_completed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
