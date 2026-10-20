from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.tasks.models import Task


TaskHandler = Callable[[Task], Any]


class TaskRegistryError(Exception):
    pass


class UnknownTaskType(TaskRegistryError):
    pass


class DuplicateTaskType(TaskRegistryError):
    pass


class InvalidTaskHandler(TaskRegistryError):
    pass

class TaskRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}
        
        
    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type is required")
        if not callable(handler):
            raise InvalidTaskHandler("handler must be callable")
        if task_type in self._handlers:
            raise DuplicateTaskType(f"task type already registered: {task_type}")

        self._handlers[task_type] = handler
        
    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError as exc:
            raise UnknownTaskType(f"Unknown task type: {task_type}") from exc
        
    def run(self, task: Task) -> Any:
        handler = self.get(task.task_type)
        return handler(task)
    
    def registered_task_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))