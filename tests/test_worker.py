import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from app.queue.manager import WorkQueue
from app.queue.worker import Worker, TaskOutcomeNotPersisted
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task, TaskStatus
from app.tasks.registry import TaskRegistry


class WorkerTests(unittest.TestCase):
    def test_worker_claims_from_repository_and_persists_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()

            def calculate(task):
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            executor = TaskExecutor(registry)

            task = Task.create("calculate", {"value": 21})
            repo.save(task)

            queue = WorkQueue()
            queue.enqueue(task.task_id)

            worker = Worker(
                worker_id="worker-01",
                queue=queue,
                task_store={},
                repository=repo,
                executor=executor,
            )

            result = worker.process_once()

            self.assertEqual(result, {"result": 42})
            persisted = repo.get(task.task_id)
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.status, TaskStatus.COMPLETED)
            self.assertEqual(persisted.worker_id, "worker-01")
            self.assertEqual(persisted.attempt_count, 1)
            self.assertEqual(persisted.result_metadata, {"result": 42})

    def test_worker_logs_and_skips_unclaimable_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            executor = TaskExecutor(TaskRegistry())
            queue = WorkQueue()
            queue.enqueue("task-ghost")

            worker = Worker(
                worker_id="worker-01",
                queue=queue,
                task_store={},
                repository=repo,
                executor=executor,
            )

            self.assertIsNone(worker.process_once())
            self.assertEqual(queue.depth, 0)

    def test_worker_logs_idle_once_until_it_picks_up_work(self):
        worker = Worker(
            worker_id="worker-01",
            queue=WorkQueue(),
            executor=TaskExecutor(TaskRegistry()),
        )

        with self.assertLogs("app.queue.worker", level="INFO") as logs:
            self.assertIsNone(worker.process_once())
            self.assertIsNone(worker.process_once())

        idle_events = [message for message in logs.output if "worker_idle" in message]
        self.assertEqual(len(idle_events), 1)

    def test_worker_raises_when_task_outcome_is_not_persisted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()

            def calculate(task):
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            executor = TaskExecutor(registry)

            task = Task.create("calculate", {"value": 21})
            task.mark_running("worker-01")
            repo.save(task)

            stale = repo.get(task.task_id)
            stale.status = TaskStatus.RUNNING
            stale.worker_id = "worker-02"
            stale.attempt_count = 2
            stale.started_at = task.started_at
            stale.updated_at = task.updated_at
            repo.save(stale)

            task.mark_completed(result_metadata={"result": 42})
            worker = Worker(
                worker_id="worker-01",
                queue=WorkQueue(),
                task_store={},
                repository=repo,
                executor=executor,
            )

            with self.assertRaises(TaskOutcomeNotPersisted):
                worker._persist_task_outcome(task)

    def test_worker_catches_executor_exception(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()

            def broken(task):
                raise RuntimeError("boom")

            registry.register("broken", broken)
            executor = TaskExecutor(registry)

            task = Task.create("broken", {"value": 1})
            repo.save(task)

            queue = WorkQueue()
            queue.enqueue(task.task_id)

            worker = Worker(
                worker_id="worker-01",
                queue=queue,
                task_store={},
                repository=repo,
                executor=executor,
            )

            self.assertIsNone(worker.process_once())
            self.assertEqual(queue.depth, 0)

    def test_worker_claims_retry_ready_task_from_database_when_queue_is_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()

            def calculate(task):
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            executor = TaskExecutor(registry)

            task = Task.create("calculate", {"value": 21})
            task.status = TaskStatus.RETRY_PENDING
            task.attempt_count = 1
            task.next_retry_at = datetime.now(timezone.utc) - timedelta(minutes=1)
            repo.save(task)

            worker = Worker(
                worker_id="worker-01",
                queue=WorkQueue(),
                task_store={},
                repository=repo,
                executor=executor,
            )

            result = worker.process_once()

            self.assertEqual(result, {"result": 42})
            persisted = repo.get(task.task_id)
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.status, TaskStatus.COMPLETED)
            self.assertEqual(persisted.attempt_count, 2)

    def test_worker_recovers_stale_running_task_from_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()

            def calculate(task):
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            executor = TaskExecutor(registry)

            task = Task.create("calculate", {"value": 21})
            task.mark_running("worker-01")
            task.started_at = datetime.now(timezone.utc) - timedelta(minutes=10)
            task.lease_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
            repo.save(task)
            repo.record_attempt(
                task_id=task.task_id,
                worker_id="worker-01",
                attempt_number=task.attempt_count,
                started_at=task.started_at,
                outcome="STARTED",
            )

            worker = Worker(
                worker_id="worker-02",
                queue=WorkQueue(),
                task_store={},
                repository=repo,
                executor=executor,
            )

            result = worker.process_once()

            self.assertEqual(result, {"result": 42})
            persisted = repo.get(task.task_id)
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.status, TaskStatus.COMPLETED)
            self.assertEqual(persisted.worker_id, "worker-02")
            self.assertEqual(persisted.attempt_count, 2)
            attempts = repo.get_task_attempts(task.task_id)
            self.assertEqual([attempt["outcome"] for attempt in attempts], ["INTERRUPTED", "SUCCESS"])

    def test_worker_renews_task_lease_while_handler_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "taskflow.db"
            repo = TaskRepository(db_path)
            registry = TaskRegistry()
            renewal_observed = Event()
            original_renewal = repo.renew_task_lease

            def track_renewal(*args, **kwargs):
                renewal_observed.set()
                return original_renewal(*args, **kwargs)

            repo.renew_task_lease = track_renewal

            def calculate(task):
                self.assertTrue(renewal_observed.wait(timeout=1))
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            task = Task.create("calculate", {"value": 21})
            repo.save(task)
            queue = WorkQueue()
            queue.enqueue(task.task_id)
            worker = Worker(
                worker_id="worker-01",
                queue=queue,
                repository=repo,
                executor=TaskExecutor(registry),
                lease_duration_seconds=0.1,
                lease_renewal_interval_seconds=0.01,
            )

            self.assertEqual(worker.process_once(), {"result": 42})
            persisted = repo.get(task.task_id)
            self.assertTrue(renewal_observed.is_set())
            self.assertEqual(persisted.status, TaskStatus.COMPLETED)
            self.assertIsNone(persisted.lease_expires_at)


if __name__ == "__main__":
    unittest.main()
