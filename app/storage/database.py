from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    next_retry_at TEXT,
    last_error TEXT,
    result_key TEXT,
    result_metadata TEXT,
    worker_id TEXT,
    lease_expires_at TEXT,
    idempotency_key TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    worker_id TEXT,
    attempt_number INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT NOT NULL,
    error TEXT,
    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_retry ON tasks(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_tasks_running ON tasks(status, started_at);
CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at);
CREATE INDEX IF NOT EXISTS idx_task_attempts_task_id ON task_attempts(task_id);
"""


def _migrate_legacy_task_attempts_uniqueness(conn: sqlite3.Connection) -> None:
    duplicate_rows = conn.execute(
        """
        SELECT task_id, attempt_number, MIN(id) AS keep_id
        FROM task_attempts
        GROUP BY task_id, attempt_number
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    for row in duplicate_rows:
        duplicate_ids = conn.execute(
            """
            SELECT id
            FROM task_attempts
            WHERE task_id = ? AND attempt_number = ? AND id != ?
            ORDER BY id ASC
            """,
            (row["task_id"], row["attempt_number"], row["keep_id"]),
        ).fetchall()
        if not duplicate_ids:
            continue
        ids_to_delete = tuple(item["id"] for item in duplicate_ids)
        placeholders = ", ".join("?" for _ in ids_to_delete)
        conn.execute(f"DELETE FROM task_attempts WHERE id IN ({placeholders})", ids_to_delete)

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_task_attempts_unique_number ON task_attempts(task_id, attempt_number)"
    )


def connect(database_path: str | Path) -> sqlite3.Connection:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn

@contextmanager
def transaction(database_path: str | Path):
    conn = connect(database_path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()
        
def _migrate_legacy_datetime_normalization(conn: sqlite3.Connection) -> None:
    if conn.execute("PRAGMA user_version").fetchone()[0] >= 1:
        return

    task_rows = conn.execute(
        "SELECT task_id, created_at, started_at, completed_at, next_retry_at, updated_at FROM tasks"
    ).fetchall()
    for row in task_rows:
        updates: dict[str, str] = {}
        for column in ("created_at", "started_at", "completed_at", "next_retry_at", "updated_at"):
            raw_value = row[column]
            if raw_value is None:
                continue
            parsed = datetime.fromisoformat(raw_value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            normalized = parsed.astimezone(timezone.utc)
            if normalized.isoformat() != raw_value:
                updates[column] = normalized.isoformat()
        if updates:
            assignments = ", ".join(f"{column} = ?" for column in updates)
            values = [updates[column] for column in updates] + [row["task_id"]]
            conn.execute(f"UPDATE tasks SET {assignments} WHERE task_id = ?", values)

    attempt_rows = conn.execute(
        "SELECT id, started_at, completed_at FROM task_attempts"
    ).fetchall()
    for row in attempt_rows:
        updates: dict[str, str] = {}
        for column in ("started_at", "completed_at"):
            raw_value = row[column]
            if raw_value is None:
                continue
            parsed = datetime.fromisoformat(raw_value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            normalized = parsed.astimezone(timezone.utc)
            if normalized.isoformat() != raw_value:
                updates[column] = normalized.isoformat()
        if updates:
            assignments = ", ".join(f"{column} = ?" for column in updates)
            values = [updates[column] for column in updates] + [row["id"]]
            conn.execute(f"UPDATE task_attempts SET {assignments} WHERE id = ?", values)

    conn.execute("PRAGMA user_version = 1;")


def _migrate_task_leases(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    if "lease_expires_at" not in columns:
        conn.execute("ALTER TABLE tasks ADD COLUMN lease_expires_at TEXT")
    conn.execute("PRAGMA user_version = 2;")


def initialize_database(database_path: str | Path) -> None:
    with transaction(database_path) as conn:
        conn.executescript(SCHEMA)
        _migrate_legacy_task_attempts_uniqueness(conn)
        _migrate_legacy_datetime_normalization(conn)
        _migrate_task_leases(conn)
