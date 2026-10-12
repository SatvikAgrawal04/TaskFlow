from __future__ import annotations

import logging
from typing import Any

from app.logging_utils import log_event
from app.queue.manager import WorkQueue, WorkQueueEmpty
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task


logger = logging.getLogger(__name__)


class TaskNotFound(Exception):
    pass


class Worker:
    def __init__(
        self,
        *,
        worker_id: str,
        queue: WorkQueue,
        task_store: dict[str, Task],
        executor: TaskExecutor,
    ) -> None:
        self.worker_id = worker_id
        self.queue = queue
        self.task_store = task_store
        self.executor = executor

    def process_once(self) -> Any | None:
        try:
            task_id = self.queue.dequeue()
        except WorkQueueEmpty:
            log_event(logger, "worker_idle", worker_id=self.worker_id)
            return None

        try:
            task = self.task_store.get(task_id)
            if task is None:
                raise TaskNotFound(f"task not found: {task_id}")
            log_event(
                logger,
                "worker_picked_task",
                worker_id=self.worker_id,
                task_id=task.task_id,
                task_type=task.task_type,
            )

            return self.executor.execute(task, worker_id=self.worker_id)
        finally:
            self.queue.task_done()