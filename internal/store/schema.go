package store

const Schema = `
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    config_json     TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'created',
    created_at      INTEGER NOT NULL,
    started_at      INTEGER,
    paused_at       INTEGER,
    finished_at     INTEGER,
    elapsed_seconds INTEGER NOT NULL DEFAULT 0,
    total_requests  INTEGER NOT NULL DEFAULT 0,
    total_success   INTEGER NOT NULL DEFAULT 0,
    total_failed    INTEGER NOT NULL DEFAULT 0,
    unique_ips      INTEGER NOT NULL DEFAULT 0,
    unique_countries INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    report_cache    TEXT,
    report_cache_ts INTEGER,
    sweep_next_sessid INTEGER
);

CREATE TABLE IF NOT EXISTS samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL,
    worker_type TEXT NOT NULL,
    sessid      INTEGER NOT NULL,
    ip          TEXT,
    country     TEXT,
    region      TEXT,
    city        TEXT,
    latency_ms  INTEGER,
    ok          INTEGER NOT NULL,
    err_kind    TEXT,
    err_msg     TEXT,
    ts          INTEGER NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_samples_task_ts ON samples(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_samples_task_ip ON samples(task_id, ip);
CREATE INDEX IF NOT EXISTS idx_samples_task_sessid ON samples(task_id, sessid);
CREATE INDEX IF NOT EXISTS idx_samples_task_ok_latency ON samples(task_id, ok, latency_ms);
CREATE INDEX IF NOT EXISTS idx_samples_task_ok_country ON samples(task_id, ok, country);
CREATE INDEX IF NOT EXISTS idx_samples_task_ok_err ON samples(task_id, ok, err_kind);

CREATE TABLE IF NOT EXISTS probe_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    sessid          INTEGER NOT NULL,
    first_ip        TEXT,
    current_ip      TEXT,
    ip_changes      INTEGER NOT NULL DEFAULT 0,
    poll_count      INTEGER NOT NULL DEFAULT 0,
    first_seen      INTEGER,
    last_seen       INTEGER,
    longest_hold_seconds INTEGER NOT NULL DEFAULT 0,
    current_ip_since INTEGER,
    UNIQUE(task_id, sessid),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metrics_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    ts              INTEGER NOT NULL,
    total_reqs      INTEGER,
    success         INTEGER,
    failed          INTEGER,
    unique_ips      INTEGER,
    hit_rate_window REAL,
    qps             REAL,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_metrics_task_ts ON metrics_snapshots(task_id, ts);

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;
PRAGMA temp_store = MEMORY;
PRAGMA foreign_keys = ON;
`
