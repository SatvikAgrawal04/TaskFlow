import tempfile
import unittest
from pathlib import Path

from app.storage.task_repository import TaskRepository
from app.tasks.models import TaskStatus
from app.tasks.service import TaskSubmissionService


class TaskSubmissionServiceTests(unittest.TestCase):
    def test_submit_creates_a_durable_pending_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = TaskRepository(Path(temp_dir) / "taskflow.db")
            service = TaskSubmissionService(repository)

            task = service.submit(
                "calculate",
                {"value": 21},
                max_attempts=5,
                idempotency_key="request-123",
            )

            persisted = repository.get(task.task_id)
            self.assertEqual(persisted.status, TaskStatus.PENDING)
            self.assertEqual(persisted.payload, {"value": 21})
            self.assertEqual(persisted.max_attempts, 5)
            self.assertEqual(persisted.idempotency_key, "request-123")


if __name__ == "__main__":
    unittest.main()
