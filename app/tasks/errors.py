from __future__ import annotations


class TaskError(Exception):
    """Base class for handler-raised task errors."""


class RetryableError(TaskError):
    """Transient failure - safe to retry."""


class PermanentError(TaskError):
    """Non-retryable failure - e.g. bad payload, unknown input."""