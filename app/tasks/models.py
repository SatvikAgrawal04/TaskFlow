from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any
from uuid import uuid4
import json

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_PENDING = "RETRY_PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    
class InvalidTaskTransition(ValueError):
    pass


ALLOWED_TRANSITIONS = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.COMPLETED, TaskStatus.RETRY_PENDING, TaskStatus.FAILED},
    TaskStatus.RETRY_PENDING: {TaskStatus.RUNNING, TaskStatus.PENDING, TaskStatus.CANCELLED, TaskStatus.FAILED},
    TaskStatus.COMPLETED: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.CANCELLED: set(),
}


@dataclass
class Task:
    task_id: str
    task_type: str
    payload: dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    next_retry_at: datetime | None = None
    last_error: str | None = None
    result_key: str | None = None
    result_metadata: dict[str, Any] | None = None
    worker_id: str | None = None
    lease_expires_at: datetime | None = None
    idempotency_key: str | None = None
    
    def __post_init__(self) -> None:
        if isinstance(self.status, str):
            self.status = TaskStatus(self.status)
        if not self.task_id:
            raise ValueError("task_id is required")
        if not self.task_type:
            raise ValueError("task_type is required")
        if not isinstance(self.payload, dict):
            raise TypeError("payload must be a dictionary")
        if self.attempt_count < 0:
            raise ValueError("attempt_count cannot be negative")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        json.dumps(self.payload)
        
    @classmethod
    def create(cls, task_type: str, payload: dict[str, Any], *, max_attempts: int = 3) -> "Task":
        return cls(task_id=str(uuid4()), task_type=task_type, payload=payload, max_attempts=max_attempts)
    
    def transition_to(self, new_status: TaskStatus) -> None:
        if new_status not in ALLOWED_TRANSITIONS[self.status]:
            raise InvalidTaskTransition(f"cannot transition from {self.status} to {new_status}")
        self.status = new_status
        self.updated_at = utc_now()
        return

    def mark_running(self, worker_id: str) -> None:
        self.transition_to(TaskStatus.RUNNING)
        self.worker_id = worker_id
        self.started_at = utc_now()
        self.attempt_count += 1
        self.next_retry_at = None
        return

    def mark_completed(self, *, result_key: str | None = None, result_metadata: dict[str, Any] | None = None) -> None:
        self.transition_to(TaskStatus.COMPLETED)
        self.completed_at = utc_now()
        self.result_key = result_key
        self.result_metadata = result_metadata
        self.last_error = None
        self.next_retry_at = None
        self.lease_expires_at = None
        return
    
    def mark_failed(self, error: str) -> None:
        self.transition_to(TaskStatus.FAILED)
        self.completed_at = utc_now()
        self.last_error = error
        self.next_retry_at = None
        self.worker_id = None
        self.lease_expires_at = None
        return
    
    def schedule_retry(self, error: str, delay_seconds: float) -> None:
        self.transition_to(TaskStatus.RETRY_PENDING)
        self.last_error = error
        self.next_retry_at = utc_now() + timedelta(seconds=delay_seconds)
        self.worker_id = None
        self.lease_expires_at = None
        return
    
    @property
    def can_retry(self) -> bool:
        return self.attempt_count < self.max_attempts
    
