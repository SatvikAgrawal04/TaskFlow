import unittest

from app.tasks.models import Task, TaskStatus, InvalidTaskTransition


class TaskModelTests(unittest.TestCase):
    def test_create_task_defaults_to_pending(self):
        task = Task.create("calculate", {"value": 42})
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(task.attempt_count, 0)

    def test_mark_running_increments_attempts(self):
        task = Task.create("calculate", {"value": 42})
        task.mark_running("worker-01")
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.attempt_count, 1)

    def test_completed_task_cannot_run_again(self):
        task = Task.create("calculate", {"value": 42})
        task.mark_running("worker-01")
        task.mark_completed(result_key="results/out.json")

        with self.assertRaises(InvalidTaskTransition):
            task.mark_running("worker-02")

    def test_retry_sets_next_retry_at(self):
        task = Task.create("calculate", {"value": 42})
        task.mark_running("worker-01")
        task.schedule_retry("temporary failure", 1)

        self.assertEqual(task.status, TaskStatus.RETRY_PENDING)
        self.assertIsNotNone(task.next_retry_at)

    def test_completed_task_clears_retry_metadata(self):
        task = Task.create("calculate", {"value": 42})
        task.mark_running("worker-01")
        task.schedule_retry("temporary failure", 15)

        task.mark_running("worker-02")
        task.mark_completed(result_metadata={"result": 42})

        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNone(task.last_error)
        self.assertIsNone(task.next_retry_at)

    def test_failed_task_clears_retry_deadline_and_worker_id(self):
        task = Task.create("calculate", {"value": 42})
        task.mark_running("worker-01")

        task.mark_failed("permanent failure")

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertIsNotNone(task.completed_at)
        self.assertIsNone(task.next_retry_at)
        self.assertIsNone(task.worker_id)


if __name__ == "__main__":
    unittest.main()