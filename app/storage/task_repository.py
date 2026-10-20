from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.storage.database import initialize_database, transaction
from app.tasks.models import Task, TaskStatus


class TaskRepository:
    """Persistence helper for Task objects backed by SQLite."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        initialize_database(self.database_path)

    def _ensure_normalized_records(self) -> None:
        with transaction(self.database_path) as conn:
            task_rows = conn.execute(
                "SELECT task_id, created_at, started_at, completed_at, next_retry_at, lease_expires_at, updated_at FROM tasks"
            ).fetchall()
            for row in task_rows:
                updates: dict[str, str] = {}
                for column in ("created_at", "started_at", "completed_at", "next_retry_at", "lease_expires_at", "updated_at"):
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

    def normalize_existing_records(self) -> None:
        self._ensure_normalized_records()

    @staticmethod
    def _normalize_datetime(value: datetime, *, field_name: str, allow_naive: bool = False) -> datetime:
        if value.tzinfo is None:
            if allow_naive:
                return value.replace(tzinfo=timezone.utc)
            raise ValueError(f"{field_name} must be timezone-aware")
        return value.astimezone(timezone.utc)

    @classmethod
    def _serialize_value(cls, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return cls._normalize_datetime(value, field_name="datetime").isoformat()
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, default=str)
        return str(value)

    @staticmethod
    def _utc_datetime(value: datetime | None, *, field_name: str) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        return TaskRepository._normalize_datetime(value, field_name=field_name)

    @staticmethod
    def _lease_expiry(now: datetime, lease_duration: timedelta | float | int) -> datetime:
        if isinstance(lease_duration, (int, float)):
            lease_duration = timedelta(seconds=lease_duration)
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        return now + lease_duration

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> Task:
        payload = json.loads(row["payload"]) if row["payload"] is not None else {}
        result_metadata = json.loads(row["result_metadata"]) if row["result_metadata"] is not None else None

        def to_utc(value: str | None) -> datetime | None:
            if value is None:
                return None
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        task = Task(
            task_id=row["task_id"],
            task_type=row["task_type"],
            payload=payload,
            status=row["status"],
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
            created_at=to_utc(row["created_at"]),
            updated_at=to_utc(row["updated_at"]),
            started_at=to_utc(row["started_at"]),
            completed_at=to_utc(row["completed_at"]),
            next_retry_at=to_utc(row["next_retry_at"]),
            last_error=row["last_error"],
            result_key=row["result_key"],
            result_metadata=result_metadata,
            worker_id=row["worker_id"],
            lease_expires_at=to_utc(row["lease_expires_at"]),
            idempotency_key=row["idempotency_key"],
        )

        return task

    def _row_to_task(self, row: sqlite3.Row | None) -> Task | None:
        if row is None:
            return None
        return self._task_from_row(row)

    def create(self, task: Task) -> Task:
        """Insert a task record and reject duplicate task IDs."""
        with transaction(self.database_path) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO tasks (
                        task_id, task_type, payload, status, attempt_count, max_attempts,
                        created_at, started_at, completed_at, next_retry_at, last_error,
                        result_key, result_metadata, worker_id, lease_expires_at, idempotency_key, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task.task_id,
                        task.task_type,
                        json.dumps(task.payload, default=str),
                        task.status.value if isinstance(task.status, TaskStatus) else str(task.status),
                        task.attempt_count,
                        task.max_attempts,
                        self._serialize_value(task.created_at),
                        self._serialize_value(task.started_at),
                        self._serialize_value(task.completed_at),
                        self._serialize_value(task.next_retry_at),
                        task.last_error,
                        task.result_key,
                        self._serialize_value(task.result_metadata),
                        task.worker_id,
                        self._serialize_value(task.lease_expires_at),
                        task.idempotency_key,
                        self._serialize_value(task.updated_at),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if "tasks.task_id" in str(exc):
                    raise ValueError(f"task already exists: {task.task_id}") from exc
                raise
        return task

    def save(self, task: Task) -> Task:
        """Insert or update a task record in SQLite.

        This method is intentionally limited to controlled, explicit task creation or
        repository-managed state restoration. Lifecycle callers should prefer the
        conditional claim/completion/retry/failure methods in this class.
        """
        with transaction(self.database_path) as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, task_type, payload, status, attempt_count, max_attempts,
                    created_at, started_at, completed_at, next_retry_at, last_error,
                    result_key, result_metadata, worker_id, lease_expires_at, idempotency_key, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    task_type = excluded.task_type,
                    payload = excluded.payload,
                    status = excluded.status,
                    attempt_count = excluded.attempt_count,
                    max_attempts = excluded.max_attempts,
                    created_at = excluded.created_at,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    next_retry_at = excluded.next_retry_at,
                    last_error = excluded.last_error,
                    result_key = excluded.result_key,
                    result_metadata = excluded.result_metadata,
                    worker_id = excluded.worker_id,
                    lease_expires_at = excluded.lease_expires_at,
                    idempotency_key = excluded.idempotency_key,
                    updated_at = excluded.updated_at
                """,
                (
                    task.task_id,
                    task.task_type,
                    json.dumps(task.payload, default=str),
                    task.status.value if isinstance(task.status, TaskStatus) else str(task.status),
                    task.attempt_count,
                    task.max_attempts,
                    self._serialize_value(task.created_at),
                    self._serialize_value(task.started_at),
                    self._serialize_value(task.completed_at),
                    self._serialize_value(task.next_retry_at),
                    task.last_error,
                    task.result_key,
                    self._serialize_value(task.result_metadata),
                    task.worker_id,
                    self._serialize_value(task.lease_expires_at),
                    task.idempotency_key,
                    self._serialize_value(task.updated_at),
                ),
            )
        return task

    def update(self, task: Task) -> Task:
        return self.save(task)

    def record_attempt(
        self,
        *,
        task_id: str,
        worker_id: str | None,
        attempt_number: int,
        started_at: datetime,
        completed_at: datetime | None = None,
        outcome: str,
        error: str | None = None,
    ) -> sqlite3.Row:
        normalized_started_at = self._normalize_datetime(started_at, field_name="started_at")
        normalized_completed_at = None if completed_at is None else self._normalize_datetime(completed_at, field_name="completed_at")
        with transaction(self.database_path) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO task_attempts (
                        task_id, worker_id, attempt_number, started_at, completed_at, outcome, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        worker_id,
                        attempt_number,
                        self._serialize_value(normalized_started_at),
                        self._serialize_value(normalized_completed_at),
                        outcome,
                        error,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"attempt already exists for task {task_id}, attempt {attempt_number}") from exc
            row = conn.execute(
                "SELECT * FROM task_attempts WHERE task_id = ? AND attempt_number = ?",
                (task_id, attempt_number),
            ).fetchone()
        return row

    @staticmethod
    def _require_attempt_update(conn: sqlite3.Connection, *, task_id: str, attempt_number: int, message: str) -> None:
        cursor = conn.execute(
            "SELECT 1 FROM task_attempts WHERE task_id = ? AND attempt_number = ?",
            (task_id, attempt_number),
        )
        if cursor.fetchone() is None:
            raise RuntimeError(
                f"Attempt record missing for task {task_id}, attempt {attempt_number}; {message}"
            )

    def get_task_attempts(self, task_id: str) -> list[sqlite3.Row]:
        with transaction(self.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM task_attempts
                WHERE task_id = ?
                ORDER BY attempt_number ASC, id ASC
                """,
                (task_id,),
            ).fetchall()
        return rows

    def list_attempts(self, task_id: str) -> list[sqlite3.Row]:
        return self.get_task_attempts(task_id)

    def get(self, task_id: str) -> Task | None:
        with transaction(self.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row)

    def get_by_id(self, task_id: str) -> Task | None:
        return self.get(task_id)

    def get_task(self, task_id: str) -> Task | None:
        return self.get(task_id)

    def claim_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_duration: timedelta | float | int = timedelta(seconds=30),
    ) -> Task | None:
        """Atomically claim a specific task identified by task_id if it is eligible."""
        cutoff = self._utc_datetime(now, field_name="now")
        lease_expiry = self._lease_expiry(cutoff, lease_duration)
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    worker_id = ?,
                    started_at = ?,
                    updated_at = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    next_retry_at = NULL
                WHERE task_id = ?
                  AND attempt_count < max_attempts
                  AND (
                        status = ?
                        OR (
                            status = ?
                            AND next_retry_at IS NOT NULL
                            AND next_retry_at <= ?
                        )
                  )
                RETURNING *
                """,
                (
                    TaskStatus.RUNNING.value,
                    worker_id,
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    lease_expiry.isoformat(),
                    task_id,
                    TaskStatus.PENDING.value,
                    TaskStatus.RETRY_PENDING.value,
                    cutoff.isoformat(),
                ),
            ).fetchone()
            if row is None:
                return None

            attempt_number = int(row["attempt_count"])
            conn.execute(
                """
                INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, completed_at, outcome, error)
                VALUES (?, ?, ?, ?, NULL, ?, NULL)
                """,
                (
                    task_id,
                    worker_id,
                    attempt_number,
                    cutoff.isoformat(),
                    "STARTED",
                ),
            )
            return self._row_to_task(row)

    def renew_task_lease(
        self,
        task_id: str,
        *,
        worker_id: str,
        attempt_number: int,
        now: datetime | None = None,
        lease_duration: timedelta | float | int = timedelta(seconds=30),
    ) -> Task | None:
        """Extend a lease only while its worker still owns the active attempt."""
        cutoff = self._utc_datetime(now, field_name="now")
        lease_expiry = self._lease_expiry(cutoff, lease_duration)
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET lease_expires_at = ?, updated_at = ?
                WHERE task_id = ?
                  AND status = ?
                  AND worker_id = ?
                  AND attempt_count = ?
                RETURNING *
                """,
                (
                    lease_expiry.isoformat(),
                    cutoff.isoformat(),
                    task_id,
                    TaskStatus.RUNNING.value,
                    worker_id,
                    attempt_number,
                ),
            ).fetchone()
        return self._row_to_task(row)

    def complete_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        attempt_number: int,
        result_key: str | None = None,
        result_metadata: dict[str, Any] | None = None,
        completed_at: datetime | None = None,
        outcome_at: datetime | None = None,
    ) -> Task | None:
        """Persist a successful outcome only if this worker still owns the same RUNNING attempt."""
        outcome_time = self._normalize_datetime(outcome_at, field_name="outcome_at") if outcome_at is not None else (
            self._normalize_datetime(completed_at, field_name="completed_at") if completed_at is not None else datetime.now(timezone.utc)
        )
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    completed_at = ?,
                    result_key = ?,
                    result_metadata = ?,
                    updated_at = ?,
                    worker_id = ?,
                    last_error = NULL,
                    next_retry_at = NULL,
                    lease_expires_at = NULL
                WHERE task_id = ?
                  AND status = ?
                  AND worker_id = ?
                  AND attempt_count = ?
                RETURNING *
                """,
                (
                    TaskStatus.COMPLETED.value,
                    outcome_time.isoformat(),
                    result_key,
                    json.dumps(result_metadata, default=str) if result_metadata is not None else None,
                    outcome_time.isoformat(),
                    worker_id,
                    task_id,
                    TaskStatus.RUNNING.value,
                    worker_id,
                    attempt_number,
                ),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                UPDATE task_attempts
                SET worker_id = ?, completed_at = ?, outcome = ?, error = ?
                WHERE task_id = ? AND attempt_number = ?
                """,
                (
                    worker_id,
                    outcome_time.isoformat(),
                    "SUCCESS",
                    None,
                    task_id,
                    attempt_number,
                ),
            )
            if cursor.rowcount != 1:
                self._require_attempt_update(
                    conn,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    message="cannot persist completion.",
                )
            return self._row_to_task(row)

    def schedule_retry_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        attempt_number: int,
        error: str,
        retry_at: datetime,
        outcome_at: datetime | None = None,
    ) -> Task | None:
        """Persist a retry outcome only if this worker still owns the same RUNNING attempt."""
        normalized_retry_at = self._normalize_datetime(retry_at, field_name="retry_at")
        outcome_time = self._normalize_datetime(outcome_at, field_name="outcome_at") if outcome_at is not None else datetime.now(timezone.utc)
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?,
                    worker_id = NULL,
                    lease_expires_at = NULL
                WHERE task_id = ?
                  AND status = ?
                  AND worker_id = ?
                  AND attempt_count = ?
                RETURNING *
                """,
                (
                    TaskStatus.RETRY_PENDING.value,
                    normalized_retry_at.isoformat(),
                    error,
                    outcome_time.isoformat(),
                    task_id,
                    TaskStatus.RUNNING.value,
                    worker_id,
                    attempt_number,
                ),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                UPDATE task_attempts
                SET worker_id = ?, completed_at = ?, outcome = ?, error = ?
                WHERE task_id = ? AND attempt_number = ?
                """,
                (
                    worker_id,
                    outcome_time.isoformat(),
                    "RETRY",
                    error,
                    task_id,
                    attempt_number,
                ),
            )
            if cursor.rowcount != 1:
                self._require_attempt_update(
                    conn,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    message="cannot persist retry state.",
                )
            return self._row_to_task(row)

    def schedule_retry(
        self,
        task_id: str,
        *,
        worker_id: str,
        attempt_number: int,
        error: str,
        retry_at: datetime,
        outcome_at: datetime | None = None,
    ) -> Task | None:
        return self.schedule_retry_task(
            task_id,
            worker_id=worker_id,
            attempt_number=attempt_number,
            error=error,
            retry_at=retry_at,
            outcome_at=outcome_at,
        )

    def fail_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        attempt_number: int,
        error: str,
        failed_at: datetime | None = None,
        outcome_at: datetime | None = None,
    ) -> Task | None:
        """Persist a permanent failure only if this worker still owns the same RUNNING attempt."""
        outcome_time = self._normalize_datetime(outcome_at, field_name="outcome_at") if outcome_at is not None else (
            self._normalize_datetime(failed_at, field_name="failed_at") if failed_at is not None else datetime.now(timezone.utc)
        )
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    completed_at = ?,
                    last_error = ?,
                    updated_at = ?,
                    next_retry_at = NULL,
                    worker_id = NULL,
                    lease_expires_at = NULL
                WHERE task_id = ?
                  AND status = ?
                  AND worker_id = ?
                  AND attempt_count = ?
                RETURNING *
                """,
                (
                    TaskStatus.FAILED.value,
                    outcome_time.isoformat(),
                    error,
                    outcome_time.isoformat(),
                    task_id,
                    TaskStatus.RUNNING.value,
                    worker_id,
                    attempt_number,
                ),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                UPDATE task_attempts
                SET worker_id = ?, completed_at = ?, outcome = ?, error = ?
                WHERE task_id = ? AND attempt_number = ?
                """,
                (
                    worker_id,
                    outcome_time.isoformat(),
                    "FAILED",
                    error,
                    task_id,
                    attempt_number,
                ),
            )
            if cursor.rowcount != 1:
                self._require_attempt_update(
                    conn,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    message="cannot persist failure.",
                )
            return self._row_to_task(row)

    def save_task(self, task: Task) -> Task:
        return self.save(task)

    def list_tasks(self, *, status: TaskStatus | str | None = None) -> list[Task]:
        query = "SELECT * FROM tasks"
        params: list[Any] = []
        if status is not None:
            status_value = status.value if isinstance(status, TaskStatus) else str(status)
            query += " WHERE status = ?"
            params.append(status_value)
        query += " ORDER BY created_at ASC"

        with transaction(self.database_path) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._task_from_row(row) for row in rows]

    def list_all(self) -> list[Task]:
        return self.list_tasks()

    def list_pending(self) -> list[Task]:
        return self.list_tasks(status=TaskStatus.PENDING)

    def get_pending_tasks(self) -> list[Task]:
        return self.list_pending()

    def list_all_tasks(self) -> list[Task]:
        return self.list_all()

    def list_retry_ready(self, *, now: datetime | None = None) -> list[Task]:
        cutoff = self._utc_datetime(now, field_name="now")
        with transaction(self.database_path) as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ? ORDER BY next_retry_at ASC",
                (TaskStatus.RETRY_PENDING.value, cutoff.isoformat()),
            ).fetchall()
        return [self._task_from_row(row) for row in rows]

    def list_dispatchable_task_ids(self, *, now: datetime | None = None) -> list[str]:
        """Return durable task IDs that the dispatcher may offer to the work queue."""
        cutoff = self._utc_datetime(now, field_name="now")
        with transaction(self.database_path) as conn:
            rows = conn.execute(
                """
                SELECT task_id
                FROM tasks
                WHERE status = ?
                   OR (status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ?)
                ORDER BY created_at ASC, task_id ASC
                """,
                (
                    TaskStatus.PENDING.value,
                    TaskStatus.RETRY_PENDING.value,
                    cutoff.isoformat(),
                ),
            ).fetchall()
        return [str(row["task_id"]) for row in rows]

    def claim_ready_task(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_duration: timedelta | float | int = timedelta(seconds=30),
    ) -> Task | None:
        """Atomically claim the next eligible PENDING task for execution."""
        cutoff = self._utc_datetime(now, field_name="now")
        lease_expiry = self._lease_expiry(cutoff, lease_duration)
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    worker_id = ?,
                    started_at = ?,
                    updated_at = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    next_retry_at = NULL
                WHERE task_id = (
                    SELECT task_id FROM tasks
                    WHERE status = ? AND attempt_count < max_attempts
                    ORDER BY created_at ASC, task_id ASC
                    LIMIT 1
                )
                RETURNING *
                """,
                (
                    TaskStatus.RUNNING.value,
                    worker_id,
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    lease_expiry.isoformat(),
                    TaskStatus.PENDING.value,
                ),
            ).fetchone()
            if row is None:
                return None
            attempt_number = int(row["attempt_count"])
            conn.execute(
                """
                INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, completed_at, outcome, error)
                VALUES (?, ?, ?, ?, NULL, ?, NULL)
                """,
                (
                    row["task_id"],
                    worker_id,
                    attempt_number,
                    cutoff.isoformat(),
                    "STARTED",
                ),
            )
            return self._row_to_task(row)

    def claim_retry_ready_task(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_duration: timedelta | float | int = timedelta(seconds=30),
    ) -> Task | None:
        """Atomically claim the next retry-ready task whose retry time has passed."""
        cutoff = self._utc_datetime(now, field_name="now")
        lease_expiry = self._lease_expiry(cutoff, lease_duration)
        with transaction(self.database_path) as conn:
            row = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    worker_id = ?,
                    started_at = ?,
                    updated_at = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    next_retry_at = NULL
                WHERE task_id = (
                    SELECT task_id FROM tasks
                    WHERE status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ? AND attempt_count < max_attempts
                    ORDER BY next_retry_at ASC, created_at ASC, task_id ASC
                    LIMIT 1
                )
                RETURNING *
                """,
                (
                    TaskStatus.RUNNING.value,
                    worker_id,
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    lease_expiry.isoformat(),
                    TaskStatus.RETRY_PENDING.value,
                    cutoff.isoformat(),
                ),
            ).fetchone()
            if row is None:
                return None
            attempt_number = int(row["attempt_count"])
            conn.execute(
                """
                INSERT INTO task_attempts (task_id, worker_id, attempt_number, started_at, completed_at, outcome, error)
                VALUES (?, ?, ?, ?, NULL, ?, NULL)
                """,
                (
                    row["task_id"],
                    worker_id,
                    attempt_number,
                    cutoff.isoformat(),
                    "STARTED",
                ),
            )
            return self._row_to_task(row)

    def recover_stale_running_tasks(
        self,
        *,
        now: datetime | None = None,
    ) -> list[Task]:
        """Recover RUNNING tasks whose worker lease has expired.

        A stale task is returned to PENDING when it has attempts remaining. Otherwise
        it is terminally failed. The caller may subsequently claim recovered pending
        work through the usual atomic claim methods.
        """
        cutoff = self._utc_datetime(now, field_name="now")
        recovery_error = "Task execution interrupted after its worker lease expired."
        recovered_tasks: list[Task] = []

        with transaction(self.database_path) as conn:
            candidates = conn.execute(
                """
                SELECT task_id, attempt_count, max_attempts
                FROM tasks
                WHERE status = ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                ORDER BY lease_expires_at ASC, created_at ASC, task_id ASC
                """,
                (
                    TaskStatus.RUNNING.value,
                    cutoff.isoformat(),
                ),
            ).fetchall()

            for candidate in candidates:
                task_id = candidate["task_id"]
                attempt_number = int(candidate["attempt_count"])
                target_status = (
                    TaskStatus.PENDING
                    if attempt_number < int(candidate["max_attempts"])
                    else TaskStatus.FAILED
                )
                row = conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?,
                        completed_at = CASE WHEN ? = ? THEN ? ELSE NULL END,
                        last_error = ?,
                        updated_at = ?,
                        next_retry_at = NULL,
                        worker_id = NULL,
                        lease_expires_at = NULL
                    WHERE task_id = ?
                      AND status = ?
                      AND attempt_count = ?
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at <= ?
                    RETURNING *
                    """,
                    (
                        target_status.value,
                        target_status.value,
                        TaskStatus.FAILED.value,
                        cutoff.isoformat(),
                        recovery_error,
                        cutoff.isoformat(),
                        task_id,
                        TaskStatus.RUNNING.value,
                        attempt_number,
                        cutoff.isoformat(),
                    ),
                ).fetchone()
                if row is None:
                    continue

                cursor = conn.execute(
                    """
                    UPDATE task_attempts
                    SET completed_at = ?, outcome = ?, error = ?
                    WHERE task_id = ? AND attempt_number = ? AND outcome = ?
                    """,
                    (
                        cutoff.isoformat(),
                        "INTERRUPTED",
                        recovery_error,
                        task_id,
                        attempt_number,
                        "STARTED",
                    ),
                )
                if cursor.rowcount != 1:
                    self._require_attempt_update(
                        conn,
                        task_id=task_id,
                        attempt_number=attempt_number,
                        message="cannot record interrupted recovery state.",
                    )
                    raise RuntimeError(
                        f"Attempt {attempt_number} for task {task_id} is not in a recoverable STARTED state."
                    )
                recovered_tasks.append(self._row_to_task(row))

        return recovered_tasks

    def claim_stale_running_task(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
    ) -> Task | None:
        """Recover expired leases, then atomically claim one recovered pending task."""
        recovered_tasks = self.recover_stale_running_tasks(now=now)
        for task in recovered_tasks:
            if task.status == TaskStatus.PENDING:
                claimed = self.claim_task(task.task_id, worker_id=worker_id, now=now)
                if claimed is not None:
                    return claimed
        return None

    def delete(self, task_id: str) -> bool:
        with transaction(self.database_path) as conn:
            try:
                cursor = conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            except sqlite3.IntegrityError:
                return False
        return cursor.rowcount > 0

    def __len__(self) -> int:
        with transaction(self.database_path) as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM tasks").fetchone()
        return int(row["total"])


__all__ = ["TaskRepository"]
