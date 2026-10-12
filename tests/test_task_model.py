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


if __name__ == "__main__":
    unittest.main()