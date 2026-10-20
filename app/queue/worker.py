from __future__ import annotations

import logging
from threading import Event, Thread
from collections.abc import Callable
from typing import Any

from app.logging_utils import log_event
from app.queue.manager import WorkQueue, WorkQueueEmpty
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task, TaskStatus


logger = logging.getLogger(__name__)


class TaskNotFound(Exception):
    pass


class TaskOutcomeNotPersisted(RuntimeError):
    """Raised when the repository rejects a task outcome because ownership changed."""


class Worker:
    def __init__(
        self,
        *,
        worker_id: str,
        queue: WorkQueue,
        task_store: dict[str, Task] | None = None,
        repository: TaskRepository | None = None,
        executor: TaskExecutor,
        lease_duration_seconds: float = 30.0,
        lease_renewal_interval_seconds: float = 10.0,
        queue_poll_timeout_seconds: float | None = None,
        claim_database_when_idle: bool = True,
        on_queue_item_finished: Callable[[str], None] | None = None,
    ) -> None:
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")
        if lease_renewal_interval_seconds <= 0:
            raise ValueError("lease_renewal_interval_seconds must be positive")
        if lease_renewal_interval_seconds >= lease_duration_seconds:
            raise ValueError("lease_renewal_interval_seconds must be shorter than lease_duration_seconds")
        if queue_poll_timeout_seconds is not None and queue_poll_timeout_seconds <= 0:
            raise ValueError("queue_poll_timeout_seconds must be positive when set")
        self.worker_id = worker_id
        self.queue = queue
        self.task_store = ( task_store if task_store is not None else {} )
        self.repository = repository
        self.executor = executor
        self.lease_duration_seconds = lease_duration_seconds
        self.lease_renewal_interval_seconds = lease_renewal_interval_seconds
        self.queue_poll_timeout_seconds = queue_poll_timeout_seconds
        self.claim_database_when_idle = claim_database_when_idle
        self.on_queue_item_finished = on_queue_item_finished
        self._is_idle = False

    def _log_idle_once(self) -> None:
        if not self._is_idle:
            log_event(logger, "worker_idle", worker_id=self.worker_id)
            self._is_idle = True

    def _start_lease_heartbeat(self, task: Task) -> tuple[Event, Thread] | None:
        if self.repository is None:
            return None

        stop_event = Event()

        def renew_lease() -> None:
            while not stop_event.wait(self.lease_renewal_interval_seconds):
                try:
                    renewed = self.repository.renew_task_lease(
                        task.task_id,
                        worker_id=self.worker_id,
                        attempt_number=task.attempt_count,
                        lease_duration=self.lease_duration_seconds,
                    )
                except Exception:
                    logger.exception(
                        "Task lease renewal failed for task %s on worker %s.",
                        task.task_id,
                        self.worker_id,
                    )
                    return
                if renewed is None:
                    log_event(
                        logger,
                        "worker_lease_renewal_rejected",
                        worker_id=self.worker_id,
                        task_id=task.task_id,
                        attempt_number=task.attempt_count,
                    )
                    return

        heartbeat = Thread(
            target=renew_lease,
            name=f"task-lease-{task.task_id}",
            daemon=True,
        )
        heartbeat.start()
        return stop_event, heartbeat

    @staticmethod
    def _stop_lease_heartbeat(heartbeat: tuple[Event, Thread] | None) -> None:
        if heartbeat is None:
            return
        stop_event, thread = heartbeat
        stop_event.set()
        thread.join()

    def _persist_outcome(self, task: Task) -> None:
        if self.repository is None:
            return

        if task.status == TaskStatus.COMPLETED:
            updated_task = self.repository.complete_task(
                task_id=task.task_id,
                worker_id=self.worker_id,
                attempt_number=task.attempt_count,
                result_key=task.result_key,
                result_metadata=task.result_metadata,
                completed_at=task.completed_at,
            )
            if updated_task is None:
                log_event(
                    logger,
                    "worker_task_outcome_not_persisted",
                    worker_id=self.worker_id,
                    task_id=task.task_id,
                    attempt_number=task.attempt_count,
                    status=task.status.value,
                )
                raise TaskOutcomeNotPersisted(
                    f"Task {task.task_id} completed but was not persisted for worker {self.worker_id}."
                )
            return

        if task.status == TaskStatus.RETRY_PENDING:
            updated_task = self.repository.schedule_retry_task(
                task_id=task.task_id,
                worker_id=self.worker_id,
                attempt_number=task.attempt_count,
                error=task.last_error or "retry",
                retry_at=task.next_retry_at or task.completed_at or task.updated_at,
            )
            if updated_task is None:
                log_event(
                    logger,
                    "worker_task_outcome_not_persisted",
                    worker_id=self.worker_id,
                    task_id=task.task_id,
                    attempt_number=task.attempt_count,
                    status=task.status.value,
                )
                raise TaskOutcomeNotPersisted(
                    f"Task {task.task_id} retry state was not persisted for worker {self.worker_id}."
                )
            return

        if task.status == TaskStatus.FAILED:
            updated_task = self.repository.fail_task(
                task_id=task.task_id,
                worker_id=self.worker_id,
                attempt_number=task.attempt_count,
                error=task.last_error or "failed",
                failed_at=task.completed_at,
            )
            if updated_task is None:
                log_event(
                    logger,
                    "worker_task_outcome_not_persisted",
                    worker_id=self.worker_id,
                    task_id=task.task_id,
                    attempt_number=task.attempt_count,
                    status=task.status.value,
                )
                raise TaskOutcomeNotPersisted(
                    f"Task {task.task_id} failure was not persisted for worker {self.worker_id}."
                )
            return

        raise ValueError(f"Task {task.task_id} has an unsupported terminal state for persistence: {task.status}")

    def _persist_task_outcome(self, task: Task) -> None:
        self._persist_outcome(task)

    def process_once(self) -> Any | None:
        task_id: str | None = None
        task: Task | None = None
        queue_item_consumed = False

        try:
            task_id = self.queue.dequeue(
                block=self.queue_poll_timeout_seconds is not None,
                timeout=self.queue_poll_timeout_seconds,
            )
            queue_item_consumed = True
        except WorkQueueEmpty:
            if self.repository is not None and self.claim_database_when_idle:
                self.repository.recover_stale_running_tasks()
                task = self.repository.claim_retry_ready_task(
                    worker_id=self.worker_id,
                    lease_duration=self.lease_duration_seconds,
                )
                if task is None:
                    task = self.repository.claim_ready_task(
                        worker_id=self.worker_id,
                        lease_duration=self.lease_duration_seconds,
                    )
                if task is not None:
                    task_id = task.task_id
                else:
                    self._log_idle_once()
                    return None
            else:
                self._log_idle_once()
                return None

        try:
            if task is None:
                if self.repository is not None:
                    task = self.repository.claim_task(
                        task_id,
                        worker_id=self.worker_id,
                        lease_duration=self.lease_duration_seconds,
                    )
                else:
                    task = self.task_store.get(task_id)
                    if task is not None:
                        if task.status == TaskStatus.RUNNING and task.worker_id is not None and task.worker_id != self.worker_id:
                            log_event(
                                logger,
                                "worker_task_unavailable",
                                worker_id=self.worker_id,
                                task_id=task.task_id,
                                reason="running_under_other_worker",
                                existing_worker_id=task.worker_id,
                            )
                            return None
                        task.mark_running(self.worker_id)

            if task is None:
                log_event(
                    logger,
                    "worker_task_unavailable",
                    worker_id=self.worker_id,
                    task_id=task_id,
                    reason="not_claimable",
                )
                return None

            log_event(
                logger,
                "worker_picked_task",
                worker_id=self.worker_id,
                task_id=task.task_id,
                task_type=task.task_type,
            )
            self._is_idle = False

            heartbeat = self._start_lease_heartbeat(task)
            try:
                result = self.executor.execute(task, worker_id=self.worker_id)
            except Exception as exc:
                self._stop_lease_heartbeat(heartbeat)
                log_event(
                    logger,
                    "worker_task_execution_failed",
                    worker_id=self.worker_id,
                    task_id=task.task_id,
                    task_type=task.task_type,
                    error=str(exc),
                    exc_info=True,
                )
                if self.repository is not None:
                    try:
                        self._persist_outcome(task)
                    except TaskOutcomeNotPersisted:
                        raise
                return None

            self._stop_lease_heartbeat(heartbeat)
            if self.repository is not None:
                self._persist_outcome(task)
            return result
        finally:
            if queue_item_consumed:
                self.queue.task_done()
                if self.on_queue_item_finished is not None and task_id is not None:
                    self.on_queue_item_finished(task_id)
