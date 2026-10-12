from __future__ import annotations

import logging
from typing import Any

from app.tasks.models import Task
from app.tasks.registry import TaskRegistry, UnknownTaskType
from app.logging_utils import log_event
from app.tasks.errors import PermanentError, RetryableError

logger = logging.getLogger(__name__)

class TaskExecutor:
    def __init__(self, registry: TaskRegistry, retry_base_delay: float = 1.0, retry_max_delay: float = 60.0) -> None:
        self.registry = registry
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        
    def _retry_delay_seconds(self, attempt_count: int) -> float:
        return min(self.retry_base_delay * (2 ** (attempt_count - 1)), self.retry_max_delay)
    
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
        
    def execute(self, task: Task, *, worker_id: str) -> Any:
        task.mark_running(worker_id)
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
            self._handle_retryable(task, exc, worker_id)
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