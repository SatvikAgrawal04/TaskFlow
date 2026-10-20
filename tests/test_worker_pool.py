import tempfile
import unittest
from pathlib import Path
from threading import Event

from app.queue.worker_pool import WorkerPool
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task, TaskStatus
from app.tasks.registry import TaskRegistry


class WorkerPoolTests(unittest.TestCase):
    def test_persisted_task_is_dispatched_and_completed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = TaskRepository(Path(temp_dir) / "taskflow.db")
            registry = TaskRegistry()
            handler_started = Event()

            def calculate(task):
                handler_started.set()
                return {"result": task.payload["value"] * 2}

            registry.register("calculate", calculate)
            pool = WorkerPool(
                repository=repo,
                executor=TaskExecutor(registry),
                worker_count=1,
                dispatcher_poll_interval_seconds=0.01,
                worker_poll_timeout_seconds=0.01,
            )
            pool.start()
            try:
                task = Task.create("calculate", {"value": 21})
                repo.save(task)

                self.assertTrue(handler_started.wait(timeout=2))
                pool.queue.join()

                persisted = repo.get(task.task_id)
                self.assertEqual(persisted.status, TaskStatus.COMPLETED)
                self.assertEqual(persisted.result_metadata, {"result": 42})
            finally:
                pool.stop()


if __name__ == "__main__":
    unittest.main()
