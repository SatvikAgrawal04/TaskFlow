from __future__ import annotations

import logging
from typing import Any

from app.tasks.models import Task, TaskStatus
from app.tasks.registry import TaskRegistry, UnknownTaskType
from app.logging_utils import log_event
from app.tasks.errors import PermanentError, RetryableError

logger = logging.getLogger(__name__)

class TaskExecutor:
    def __init__(self, registry: TaskRegistry, retry_base_delay: float = 1.0, retry_max_delay: float = 60.0) -> None:
        if retry_base_delay < 0:
            raise ValueError("retry_base_delay must be non-negative")
        if retry_max_delay < 0:
            raise ValueError("retry_max_delay must be non-negative")
        if retry_max_delay < retry_base_delay:
            raise ValueError("retry_max_delay must be greater than or equal to retry_base_delay")

        self.registry = registry
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay

    def _retry_delay_seconds(self, attempt_count: int) -> float:
        capped_attempt = max(1, min(attempt_count, 31))
        return min(self.retry_base_delay * (2 ** (capped_attempt - 1)), self.retry_max_delay)

    def _handle_retryable(self, task: Task, exc: Exception, worker_id: str) -> None:
        if task.can_retry:
            delay = self._retry_delay_seconds(task.attempt_count)
            task.schedule_retry(str(exc), delay)
            
            log_event(
                logger,
                "task_retry_scheduled",
                task_id = task.task_id,
                task_type = task.task_type,
                worker_id = worker_id,
                attempt_count = task.attempt_count,
                delay_seconds = delay,
                error = str(exc)
            )
        else:
            task.mark_failed(str(exc))
            log_event(
                logger,
                "task_failed",
                task_id=task.task_id,
                task_type=task.task_type,
                worker_id=worker_id,
                attempt_count=task.attempt_count,
                retryable=True,
                reason="retries_exhausted",
                error=str(exc),
            )
        
    def execute_in_memory_demo(self, task: Task, *, worker_id: str) -> Any:
        if task.status == TaskStatus.RUNNING:
            if task.worker_id is not None and task.worker_id != worker_id:
                raise ValueError(
                    f"Task {task.task_id} is already RUNNING for worker {task.worker_id}; "
                    f"demo execution cannot steal ownership from {worker_id}."
                )
            task.worker_id = worker_id
        else:
            task.mark_running(worker_id)
        return self.execute(task, worker_id=worker_id)

    def execute(self, task: Task, *, worker_id: str) -> Any:
        if task.status != TaskStatus.RUNNING:
            raise ValueError(
                f"Task {task.task_id} must already be RUNNING before execution; claim it before calling execute()."
            )
        if task.worker_id is not None and task.worker_id != worker_id:
            raise ValueError(
                f"Task {task.task_id} is already RUNNING for worker {task.worker_id}; "
                f"execution caller {worker_id} is not the owner."
            )

        task.worker_id = worker_id
        log_event(
            logger,
            "task_started",
            task_id=task.task_id,
            task_type=task.task_type,
            worker_id=worker_id,
            attempt_count=task.attempt_count,
        )

        try:
            result = self.registry.run(task)
        except UnknownTaskType as exc:
            task.mark_failed(str(exc))
            log_event(logger, "task_failed", task_id=task.task_id, worker_id=worker_id,
                    retryable=False, reason="unknown_task_type", error=str(exc))
            raise
        except PermanentError as exc:
            task.mark_failed(str(exc))
            log_event(logger, "task_failed", task_id=task.task_id, worker_id=worker_id,
                    retryable=False, error=str(exc))
            raise
        except RetryableError as exc:
            self._handle_retryable(task, exc, worker_id)
            raise
        except Exception as exc:
            task.mark_failed(str(exc))
            logger.exception(
                "Unexpected task exception marked permanent; task %s failed without retry",
                task.task_id,
            )
            log_event(
                logger,
                "task_failed",
                task_id=task.task_id,
                task_type=task.task_type,
                worker_id=worker_id,
                retryable=False,
                reason="unexpected_exception",
                error=str(exc),
            )
            raise

        metadata = result if isinstance(result, dict) else {"result": result}
        task.mark_completed(result_metadata=metadata)
        log_event(
            logger,
            "task_completed",
            task_id=task.task_id,
            task_type=task.task_type,
            worker_id=worker_id,
            status=task.status.value,
            result_metadata=task.result_metadata,
        )
        return result