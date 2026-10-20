from __future__ import annotations

from typing import Any

from app.storage.task_repository import TaskRepository
from app.tasks.models import Task


class TaskSubmissionService:
    """Creates durable task records for a running dispatcher to discover."""

    def __init__(self, repository: TaskRepository) -> None:
        self.repository = repository

    def submit(
        self,
        task_type: str,
        payload: dict[str, Any],
        *,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
    ) -> Task:
        task = Task.create(task_type, payload, max_attempts=max_attempts)
        task.idempotency_key = idempotency_key
        return self.repository.create(task)
