import unittest

from app.queue.manager import WorkQueue
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task, TaskStatus
from app.tasks.registry import TaskRegistry


class Phase1FlowTests(unittest.TestCase):
    def test_message_flows_from_submission_to_completed_task(self):
        message = {
            "task_type": "calculate",
            "payload": {
                "value": 21
            }
        }

        registry = TaskRegistry()
        queue = WorkQueue()
        task_store = {}

        def calculate(task):
            return {"result": task.payload["value"] * 2}

        registry.register("calculate", calculate)
        executor = TaskExecutor(registry)

        task = Task.create(
            task_type=message["task_type"],
            payload=message["payload"],
        )

        task_store[task.task_id] = task
        queue.enqueue(task.task_id)

        dequeued_task_id = queue.dequeue()
        dequeued_task = task_store[dequeued_task_id]

        result = executor.execute(dequeued_task, worker_id="worker-01")
        queue.task_done()

        self.assertEqual(result, {"result": 42})
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(task.result_metadata, {"result": 42})
        self.assertEqual(queue.depth, 0)


if __name__ == "__main__":
    unittest.main()