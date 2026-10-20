import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.storage.database import connect, initialize_database, transaction


class DatabaseTests(unittest.TestCase):
    def test_initialize_database_creates_tasks_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"

            initialize_database(db_path)

            with transaction(db_path) as conn:
                row = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tasks'"
                ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["name"], "tasks")

    def test_connection_uses_row_factory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            initialize_database(db_path)

            with transaction(db_path) as conn:
                row = conn.execute("SELECT 1 AS value").fetchone()

            self.assertIsInstance(row, sqlite3.Row)
            self.assertEqual(row["value"], 1)

    def test_initialize_database_migrates_legacy_attempt_uniqueness(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"

            with transaction(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE tasks (
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
                        idempotency_key TEXT,
                        updated_at TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE task_attempts (
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
                    """
                )
                conn.execute(
                    "INSERT INTO tasks (task_id, task_type, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("task-1", "demo", "{}", "PENDING", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
                )
                conn.execute(
                    "INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, outcome) VALUES (?, ?, ?, ?, ?)",
                    ("task-1", "worker-a", 1, "2024-01-01T00:00:00+00:00", "STARTED"),
                )
                conn.execute(
                    "INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, outcome) VALUES (?, ?, ?, ?, ?)",
                    ("task-1", "worker-b", 1, "2024-01-01T00:01:00+00:00", "RETRY"),
                )

            initialize_database(db_path)

            with transaction(db_path) as conn:
                row = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_task_attempts_unique_number'"
                ).fetchone()
                duplicates = conn.execute(
                    "SELECT task_id, attempt_number, COUNT(*) AS dup_count FROM task_attempts GROUP BY task_id, attempt_number HAVING COUNT(*) > 1"
                ).fetchall()

            self.assertIsNotNone(row)
            self.assertEqual(len(duplicates), 0)

    def test_initialize_database_normalizes_legacy_naive_timestamps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"

            with transaction(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE tasks (
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
                        idempotency_key TEXT,
                        updated_at TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE task_attempts (
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
                    """
                )
                conn.execute(
                    "INSERT INTO tasks (task_id, task_type, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("task-1", "demo", "{}", "PENDING", "2024-01-01T00:00:00", "2024-01-01T00:00:00"),
                )
                conn.execute(
                    "INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, outcome) VALUES (?, ?, ?, ?, ?)",
                    ("task-1", "worker-a", 1, "2024-01-01T00:05:00", "STARTED"),
                )

            initialize_database(db_path)

            with transaction(db_path) as conn:
                task_row = conn.execute("SELECT created_at, updated_at FROM tasks WHERE task_id = ?", ("task-1",)).fetchone()
                attempt_row = conn.execute("SELECT started_at FROM task_attempts WHERE task_id = ? AND attempt_number = ?", ("task-1", 1)).fetchone()

            self.assertTrue(task_row["created_at"].endswith("+00:00"))
            self.assertTrue(task_row["updated_at"].endswith("+00:00"))
            self.assertTrue(attempt_row["started_at"].endswith("+00:00"))


if __name__ == "__main__":
    unittest.main()