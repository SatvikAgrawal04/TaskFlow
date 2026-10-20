import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.storage.database import transaction
from app.storage.task_repository import TaskRepository
from app.tasks.models import Task, TaskStatus


class TaskRepositoryTests(unittest.TestCase):
    def test_save_and_fetch_task_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 21})
            repo.save(task)

            persisted = repo.get(task.task_id)

            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.task_id, task.task_id)
            self.assertEqual(persisted.task_type, "calculate")
            self.assertEqual(persisted.payload, {"value": 21})
            self.assertEqual(persisted.status, TaskStatus.PENDING)

    def test_update_task_persists_latest_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 2})
            repo.save(task)

            task.mark_running("worker-01")
            repo.update(task)

            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)
            self.assertEqual(persisted.worker_id, "worker-01")
            self.assertEqual(persisted.attempt_count, 1)

    def test_delete_rejects_tasks_with_attempt_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 5})
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=task.created_at,
                completed_at=task.created_at,
                outcome="SUCCESS",
                error=None,
            )

            self.assertFalse(repo.delete(task.task_id))
            self.assertIsNotNone(repo.get(task.task_id))
            self.assertEqual(len(repo.get_task_attempts(task.task_id)), 1)

    def test_claim_and_complete_update_attempt_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 9})
            repo.save(task)

            claimed = repo.claim_task(task.task_id, worker_id="worker-02")
            self.assertIsNotNone(claimed)
            attempts = repo.get_task_attempts(task.task_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["attempt_number"], 1)
            self.assertEqual(attempts[0]["outcome"], "STARTED")

            complete = repo.complete_task(
                task.task_id,
                worker_id="worker-02",
                attempt_number=1,
                result_key="done",
                result_metadata={"result": 9},
            )
            self.assertIsNotNone(complete)
            finished = repo.get_task_attempts(task.task_id)
            self.assertEqual(finished[0]["outcome"], "SUCCESS")

    def test_list_pending_tasks_filters_by_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            pending = Task.create("calculate", {"value": 1})
            retrying = Task.create("calculate", {"value": 2})
            retrying.mark_running("worker-01")
            retrying.schedule_retry("temporary failure", 0.5)
            completed = Task.create("calculate", {"value": 3})
            completed.mark_running("worker-01")
            completed.mark_completed(result_metadata={"result": 3})

            repo.save(pending)
            repo.save(retrying)
            repo.save(completed)

            pending_tasks = repo.list_pending()
            self.assertEqual({task.task_id for task in pending_tasks}, {pending.task_id})

    def test_record_attempt_tracks_execution_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 7})
            repo.save(task)

            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=task.started_at or task.created_at,
                completed_at=task.created_at,
                outcome="SUCCESS",
                error=None,
            )

            attempts = repo.get_task_attempts(task.task_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["attempt_number"], 1)
            self.assertEqual(attempts[0]["outcome"], "SUCCESS")

    def test_complete_task_requires_existing_attempt_record(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 9})
            repo.save(task)
            claimed = repo.claim_task(task.task_id, worker_id="worker-02")
            self.assertIsNotNone(claimed)

            with transaction(db_path) as conn:
                conn.execute("DELETE FROM task_attempts WHERE task_id = ?", (task.task_id,))

            with self.assertRaises(RuntimeError):
                repo.complete_task(
                    task.task_id,
                    worker_id="worker-02",
                    attempt_number=1,
                    result_key="done",
                    result_metadata={"result": 9},
                )

            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)

    def test_record_attempt_rejects_duplicate_attempt_number(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 7})
            repo.save(task)

            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=task.created_at,
                outcome="STARTED",
            )

            with self.assertRaises(ValueError):
                repo.record_attempt(
                    task_id=task.task_id,
                    worker_id="worker-02",
                    attempt_number=1,
                    started_at=task.created_at,
                    completed_at=task.created_at,
                    outcome="RETRY",
                    error="duplicate",
                )

    def test_claim_task_rejects_conflicting_attempt_record(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 7})
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-old",
                attempt_number=1,
                started_at=task.created_at,
                outcome="STARTED",
            )

            with self.assertRaises(sqlite3.IntegrityError):
                repo.claim_task(task.task_id, worker_id="worker-new")

            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.PENDING)
            self.assertEqual(persisted.worker_id, None)
            self.assertEqual(len(repo.get_task_attempts(task.task_id)), 1)

    def test_claim_ready_task_is_atomic_and_single_claim(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 11})
            repo.save(task)

            claimed = repo.claim_ready_task(worker_id="worker-01")
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.status, TaskStatus.RUNNING)
            self.assertEqual(claimed.attempt_count, 1)
            self.assertEqual(claimed.worker_id, "worker-01")

            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)
            self.assertEqual(persisted.attempt_count, 1)

            second_claim = repo.claim_ready_task(worker_id="worker-02")
            self.assertIsNone(second_claim)

    def test_claim_retry_ready_task_moves_retry_back_to_running(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 13})
            task.mark_running("worker-01")
            task.schedule_retry("temporary failure", 0.1)
            repo.save(task)

            claimed = repo.claim_retry_ready_task(worker_id="worker-02", now=task.next_retry_at)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.status, TaskStatus.RUNNING)
            self.assertEqual(claimed.worker_id, "worker-02")
            self.assertEqual(claimed.attempt_count, 2)
            self.assertIsNone(claimed.next_retry_at)

            persisted = repo.get(task.task_id)
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)
            self.assertIsNone(persisted.next_retry_at)

    def test_complete_task_clears_retry_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 21})
            task.mark_running("worker-01")
            task.schedule_retry("temporary failure", 60)
            repo.save(task)

            claimed = repo.claim_retry_ready_task(worker_id="worker-02", now=task.next_retry_at)
            self.assertIsNotNone(claimed)

            completed = repo.complete_task(
                task.task_id,
                worker_id="worker-02",
                attempt_number=2,
                result_key="done",
                result_metadata={"result": 21},
            )
            self.assertIsNotNone(completed)
            self.assertIsNone(completed.last_error)
            self.assertIsNone(completed.next_retry_at)

            persisted = repo.get(task.task_id)
            self.assertIsNotNone(persisted)
            self.assertIsNone(persisted.last_error)
            self.assertIsNone(persisted.next_retry_at)

    def test_schedule_retry_uses_retry_time_separately_from_outcome_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 99})
            task.mark_running("worker-01")
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=task.started_at or task.created_at,
                outcome="STARTED",
            )

            retry_at = datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc)
            outcome_at = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
            updated = repo.schedule_retry_task(
                task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                error="temporary failure",
                retry_at=retry_at,
                outcome_at=outcome_at,
            )

            self.assertIsNotNone(updated)
            self.assertEqual(updated.next_retry_at, retry_at)
            self.assertEqual(updated.updated_at, outcome_at)
            self.assertIsNone(updated.worker_id)

            attempt = repo.get_task_attempts(task.task_id)[0]
            self.assertEqual(datetime.fromisoformat(attempt["completed_at"]), outcome_at)

    def test_claim_task_rejects_retry_before_deadline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 99})
            task.mark_running("worker-01")
            task.schedule_retry("temporary failure", 30)
            repo.save(task)

            claimed = repo.claim_task(task.task_id, worker_id="worker-02", now=datetime(2024, 1, 1, tzinfo=timezone.utc))
            self.assertIsNone(claimed)

    def test_claim_retry_ready_task_normalizes_offset_before_comparison(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 41})
            task.status = TaskStatus.RETRY_PENDING
            task.worker_id = "worker-01"
            task.attempt_count = 1
            task.max_attempts = 3
            task.next_retry_at = datetime(2024, 1, 1, 0, 30, tzinfo=timezone(timedelta(hours=2)))
            task.updated_at = task.next_retry_at
            task.last_error = "temporary failure"
            repo.save(task)

            claimed = repo.claim_retry_ready_task(
                worker_id="worker-02",
                now=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            )

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.status, TaskStatus.RUNNING)
            self.assertEqual(claimed.worker_id, "worker-02")

    def test_claim_task_rejects_naive_now_datetime(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 9})
            repo.save(task)

            with self.assertRaises(ValueError):
                repo.claim_task(task.task_id, worker_id="worker-01", now=datetime(2024, 1, 1))

    def test_complete_task_refuses_stale_worker_outcome(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            task = Task.create("calculate", {"value": 7})
            task.mark_running("worker-01")
            repo.save(task)

            newer_state = repo.get(task.task_id)
            newer_state.status = TaskStatus.RUNNING
            newer_state.worker_id = "worker-02"
            newer_state.attempt_count = 2
            newer_state.started_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
            newer_state.updated_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
            repo.save(newer_state)

            updated = repo.complete_task(
                task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                result_key="result-7",
                result_metadata={"result": 14},
            )

            self.assertIsNone(updated)
            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)
            self.assertEqual(persisted.worker_id, "worker-02")
            self.assertEqual(persisted.attempt_count, 2)

    def test_claim_ready_task_returns_exact_row_for_same_worker_and_timestamp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)

            older_pending = Task.create("calculate", {"value": 1})
            newer_running = Task.create("calculate", {"value": 2})

            older_pending.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
            older_pending.updated_at = older_pending.created_at

            newer_running.created_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
            newer_running.updated_at = datetime(2024, 1, 3, tzinfo=timezone.utc)
            newer_running.status = TaskStatus.RUNNING
            newer_running.worker_id = "worker-01"
            newer_running.started_at = datetime(2024, 1, 3, tzinfo=timezone.utc)
            newer_running.attempt_count = 1

            repo.save(older_pending)
            repo.save(newer_running)

            now = datetime(2024, 1, 3, tzinfo=timezone.utc)
            claimed = repo.claim_ready_task(worker_id="worker-01", now=now)

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.task_id, older_pending.task_id)
            self.assertEqual(claimed.worker_id, "worker-01")
            self.assertEqual(claimed.status, TaskStatus.RUNNING)

    def test_recover_stale_running_task_preserves_interrupted_attempt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            started_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
            recovery_time = started_at + timedelta(minutes=10)

            task = Task.create("calculate", {"value": 7})
            task.mark_running("worker-01")
            task.started_at = started_at
            task.updated_at = started_at
            task.lease_expires_at = recovery_time - timedelta(minutes=1)
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=started_at,
                outcome="STARTED",
            )

            recovered = repo.recover_stale_running_tasks(now=recovery_time)

            self.assertEqual([item.task_id for item in recovered], [task.task_id])
            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.PENDING)
            self.assertIsNone(persisted.worker_id)
            self.assertEqual(persisted.attempt_count, 1)
            attempt = repo.get_task_attempts(task.task_id)[0]
            self.assertEqual(attempt["outcome"], "INTERRUPTED")
            self.assertEqual(datetime.fromisoformat(attempt["completed_at"]), recovery_time)

            claimed = repo.claim_task(task.task_id, worker_id="worker-02", now=recovery_time)
            self.assertEqual(claimed.status, TaskStatus.RUNNING)
            self.assertEqual(claimed.attempt_count, 2)

    def test_recovery_fails_stale_task_with_exhausted_attempts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            started_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
            recovery_time = started_at + timedelta(minutes=10)

            task = Task.create("calculate", {"value": 7}, max_attempts=1)
            task.mark_running("worker-01")
            task.started_at = started_at
            task.updated_at = started_at
            task.lease_expires_at = recovery_time - timedelta(minutes=1)
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=started_at,
                outcome="STARTED",
            )

            recovered = repo.recover_stale_running_tasks(now=recovery_time)

            self.assertEqual([item.status for item in recovered], [TaskStatus.FAILED])
            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.FAILED)
            self.assertIsNone(persisted.worker_id)
            self.assertEqual(persisted.completed_at, recovery_time)
            attempt = repo.get_task_attempts(task.task_id)[0]
            self.assertEqual(attempt["outcome"], "INTERRUPTED")

    def test_recovery_does_not_reclaim_a_fresh_running_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            started_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

            task = Task.create("calculate", {"value": 7})
            task.mark_running("worker-01")
            task.started_at = started_at
            task.updated_at = started_at
            task.lease_expires_at = started_at + timedelta(minutes=5)
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                started_at=started_at,
                outcome="STARTED",
            )

            recovered = repo.recover_stale_running_tasks(now=started_at + timedelta(minutes=4))

            self.assertEqual(recovered, [])
            persisted = repo.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.RUNNING)
            self.assertEqual(persisted.worker_id, "worker-01")

    def test_claim_renewal_and_completion_manage_task_lease(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            now = datetime(2024, 1, 1, tzinfo=timezone.utc)

            task = Task.create("calculate", {"value": 7})
            repo.save(task)
            claimed = repo.claim_task(
                task.task_id,
                worker_id="worker-01",
                now=now,
                lease_duration=30,
            )

            self.assertEqual(claimed.lease_expires_at, now + timedelta(seconds=30))
            renewed = repo.renew_task_lease(
                task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                now=now + timedelta(seconds=10),
                lease_duration=30,
            )
            self.assertEqual(renewed.lease_expires_at, now + timedelta(seconds=40))
            self.assertIsNone(
                repo.renew_task_lease(
                    task.task_id,
                    worker_id="worker-02",
                    attempt_number=1,
                    now=now + timedelta(seconds=11),
                    lease_duration=30,
                )
            )

            completed = repo.complete_task(
                task.task_id,
                worker_id="worker-01",
                attempt_number=1,
                result_metadata={"result": 14},
                outcome_at=now + timedelta(seconds=12),
            )
            self.assertIsNone(completed.lease_expires_at)


if __name__ == "__main__":
    unittest.main()
