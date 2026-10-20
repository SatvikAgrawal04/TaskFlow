import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.queue.manager import WorkQueue
from app.queue.worker import Worker
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task
from app.tasks.registry import TaskRegistry
from scripts.demo_output import DemoTimeline


logging.basicConfig(level=logging.WARNING)


def main():
    timeline = DemoTimeline("TaskFlow Phase 1 Demo")
    timeline.start()

    message = {
        "task_type": "calculate",
        "payload": {
            "value": 21
        }
    }

    timeline.event(
        "Message received",
        task_type=message["task_type"],
        payload=message["payload"],
    )

    registry = TaskRegistry()
    queue = WorkQueue()
    task_store = {}

    def calculate(task):
        timeline.event(
            "Handler executing",
            task_id=task.task_id,
            operation=f'{task.payload["value"]} * 2',
        )
        return {"result": task.payload["value"] * 2}

    registry.register("calculate", calculate)
    timeline.event(
        "Handler registered",
        task_type="calculate",
        registered_task_types=registry.registered_task_types(),
    )

    executor = TaskExecutor(registry)

    task = Task.create(
        task_type=message["task_type"],
        payload=message["payload"],
    )

    task_store[task.task_id] = task

    timeline.event(
        "Task created",
        task_id=task.task_id,
        task_type=task.task_type,
        status=task.status.value,
    )

    queue.enqueue(task.task_id)
    timeline.event(
        "Task enqueued",
        task_id=task.task_id,
        queue_depth=queue.depth,
    )

    worker = Worker(
        worker_id="worker-01",
        queue=queue,
        task_store=task_store,
        executor=executor,
    )

    timeline.event(
        "Worker processing next task",
        worker_id=worker.worker_id,
        queue_depth_before=queue.depth,
    )

    result = worker.process_once()

    timeline.event(
        "Task completed",
        task_id=task.task_id,
        status=task.status.value,
        attempt_count=task.attempt_count,
        result=result,
        result_metadata=task.result_metadata,
    )

    timeline.event(
        "Queue drained",
        queue_depth=queue.depth,
    )


if __name__ == "__main__":
    main()
