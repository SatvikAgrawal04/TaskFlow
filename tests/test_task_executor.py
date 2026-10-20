import unittest

from app.tasks.executor import TaskExecutor
from app.tasks.models import Task, TaskStatus
from app.tasks.registry import TaskRegistry, UnknownTaskType
from app.tasks.errors import PermanentError, RetryableError


class TaskExecutorTests(unittest.TestCase):
    def test_successful_execution_marks_task_completed(self):
        registry = TaskRegistry()

        def calculate(task):
            return {"result": task.payload["value"] * 2}

        registry.register("calculate", calculate)

        task = Task.create("calculate", {"value": 21})
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        result = executor.execute(task, worker_id="worker-01")

        self.assertEqual(result, {"result": 42})
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.worker_id, "worker-01")
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(task.result_metadata, {"result": 42})
        self.assertIsNotNone(task.started_at)
        self.assertIsNotNone(task.completed_at)

    def test_non_dict_result_is_wrapped_in_result_metadata(self):
        registry = TaskRegistry()

        def calculate(task):
            return task.payload["value"] * 2

        registry.register("calculate", calculate)

        task = Task.create("calculate", {"value": 10})
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        result = executor.execute(task, worker_id="worker-01")

        self.assertEqual(result, 20)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.result_metadata, {"result": 20})

    def test_execute_requires_running_task(self):
        registry = TaskRegistry()

        def calculate(task):
            return task.payload["value"] * 2

        registry.register("calculate", calculate)

        task = Task.create("calculate", {"value": 21})
        executor = TaskExecutor(registry)

        with self.assertRaisesRegex(ValueError, "RUNNING"):
            executor.execute(task, worker_id="worker-01")

    def test_execute_rejects_task_running_for_another_worker(self):
        registry = TaskRegistry()

        def calculate(task):
            return task.payload["value"] * 2

        registry.register("calculate", calculate)

        task = Task.create("calculate", {"value": 21})
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaisesRegex(ValueError, "worker"):
            executor.execute(task, worker_id="worker-02")

        self.assertEqual(task.attempt_count, 1)

    def test_retryable_error_schedules_retry_and_reraises_when_attempts_remain(self):
        registry = TaskRegistry()

        def broken_task(task):
            raise RetryableError("boom")

        registry.register("broken", broken_task)

        task = Task.create("broken", {}, max_attempts=3)
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(RetryableError):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.RETRY_PENDING)
        self.assertEqual(task.attempt_count, 1)
        self.assertIn("boom", task.last_error)
        self.assertIsNotNone(task.next_retry_at)

    def test_unexpected_exception_marks_failed_permanently(self):
        registry = TaskRegistry()

        def broken_task(task):
            raise TypeError("bad handler")

        registry.register("broken", broken_task)

        task = Task.create("broken", {}, max_attempts=3)
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(TypeError):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.attempt_count, 1)
        self.assertIn("bad handler", task.last_error)
        self.assertIsNone(task.next_retry_at)
        
    def test_handler_exception_marks_failed_when_attempts_are_exhausted(self):
        registry = TaskRegistry()

        def broken_task(task):
            raise RuntimeError("boom")

        registry.register("broken", broken_task)

        task = Task.create("broken", {}, max_attempts=1)
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(RuntimeError):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.attempt_count, 1)
        self.assertIn("boom", task.last_error)

    def test_retryable_error_schedules_retry(self):
        registry = TaskRegistry()

        def flaky_task(task):
            raise RetryableError("temporary outage")

        registry.register("flaky", flaky_task)

        task = Task.create("flaky", {}, max_attempts=3)
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(RetryableError):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.RETRY_PENDING)
        self.assertEqual(task.attempt_count, 1)
        self.assertIn("temporary outage", task.last_error)
        self.assertIsNotNone(task.next_retry_at)

    def test_invalid_retry_configuration_raises_value_error(self):
        registry = TaskRegistry()

        with self.assertRaisesRegex(ValueError, "retry"):
            TaskExecutor(registry, retry_base_delay=-1)

        with self.assertRaisesRegex(ValueError, "retry"):
            TaskExecutor(registry, retry_base_delay=2, retry_max_delay=1)

    def test_permanent_error_marks_failed_without_retry(self):
        registry = TaskRegistry()

        def invalid_task(task):
            raise PermanentError("bad payload")

        registry.register("invalid", invalid_task)

        task = Task.create("invalid", {}, max_attempts=3)
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(PermanentError):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.attempt_count, 1)
        self.assertIn("bad payload", task.last_error)
        self.assertIsNone(task.next_retry_at)
        
    def test_retry_delay_uses_exponential_backoff_with_cap(self):
        registry = TaskRegistry()
        executor = TaskExecutor(registry, retry_base_delay=2, retry_max_delay=5)

        self.assertEqual(executor._retry_delay_seconds(1), 2)
        self.assertEqual(executor._retry_delay_seconds(2), 4)
        self.assertEqual(executor._retry_delay_seconds(3), 5)

    def test_unknown_task_type_marks_task_failed_and_reraises(self):
        registry = TaskRegistry()
        task = Task.create("missing", {})
        task.mark_running("worker-01")
        executor = TaskExecutor(registry)

        with self.assertRaises(UnknownTaskType):
            executor.execute(task, worker_id="worker-01")

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.attempt_count, 1)


if __name__ == "__main__":
    unittest.main()