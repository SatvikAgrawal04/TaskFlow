from __future__ import annotations

import logging
from threading import Event, Lock, Thread

from app.logging_utils import log_event
from app.queue.manager import WorkQueue, WorkQueueFull
from app.queue.worker import Worker
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor


logger = logging.getLogger(__name__)


class WorkerPool:
    """Runs a durable dispatcher and a configurable group of queue-backed workers."""

    def __init__(
        self,
        *,
        repository: TaskRepository,
        executor: TaskExecutor,
        worker_count: int = 4,
        queue: WorkQueue | None = None,
        dispatcher_poll_interval_seconds: float = 0.5,
        worker_poll_timeout_seconds: float = 0.1,
        lease_duration_seconds: float = 30.0,
        lease_renewal_interval_seconds: float = 10.0,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if dispatcher_poll_interval_seconds <= 0:
            raise ValueError("dispatcher_poll_interval_seconds must be positive")
        if worker_poll_timeout_seconds <= 0:
            raise ValueError("worker_poll_timeout_seconds must be positive")

        self.repository = repository
        self.executor = executor
        self.worker_count = worker_count
        self.queue = queue or WorkQueue()
        self.dispatcher_poll_interval_seconds = dispatcher_poll_interval_seconds
        self.worker_poll_timeout_seconds = worker_poll_timeout_seconds
        self.lease_duration_seconds = lease_duration_seconds
        self.lease_renewal_interval_seconds = lease_renewal_interval_seconds
        self._stop_event = Event()
        self._scheduled_task_ids: set[str] = set()
        self._scheduled_lock = Lock()
        self._dispatcher_thread: Thread | None = None
        self._worker_threads: list[Thread] = []

    @property
    def is_running(self) -> bool:
        return self._dispatcher_thread is not None and self._dispatcher_thread.is_alive()

    def start(self) -> None:
        if self.is_running:
            raise RuntimeError("worker pool is already running")

        self._stop_event.clear()
        self._dispatcher_thread = Thread(
            target=self._run_dispatcher,
            name="task-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()
        self._worker_threads = []
        for index in range(self.worker_count):
            thread = Thread(
                target=self._run_worker,
                args=(f"worker-{index + 1}",),
                name=f"task-worker-{index + 1}",
                daemon=True,
            )
            thread.start()
            self._worker_threads.append(thread)
        log_event(logger, "worker_pool_started", worker_count=self.worker_count)

    def stop(self, *, timeout_seconds: float = 5.0) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds cannot be negative")
        self._stop_event.set()
        threads = [thread for thread in [self._dispatcher_thread, *self._worker_threads] if thread is not None]
        for thread in threads:
            thread.join(timeout=timeout_seconds)
        log_event(logger, "worker_pool_stopped", worker_count=self.worker_count)

    def _run_dispatcher(self) -> None:
        while not self._stop_event.is_set():
            self.repository.recover_stale_running_tasks()
            for task_id in self.repository.list_dispatchable_task_ids():
                with self._scheduled_lock:
                    if task_id in self._scheduled_task_ids:
                        continue
                    try:
                        self.queue.enqueue(task_id)
                    except WorkQueueFull:
                        break
                    self._scheduled_task_ids.add(task_id)
            self._stop_event.wait(self.dispatcher_poll_interval_seconds)

    def _run_worker(self, worker_id: str) -> None:
        log_event(logger, "worker_started", worker_id=worker_id)
        worker = Worker(
            worker_id=worker_id,
            queue=self.queue,
            repository=self.repository,
            executor=self.executor,
            lease_duration_seconds=self.lease_duration_seconds,
            lease_renewal_interval_seconds=self.lease_renewal_interval_seconds,
            queue_poll_timeout_seconds=self.worker_poll_timeout_seconds,
            claim_database_when_idle=False,
            on_queue_item_finished=self._release_scheduled_task,
        )
        while not self._stop_event.is_set():
            worker.process_once()
        log_event(logger, "worker_stopped", worker_id=worker_id)

    def _release_scheduled_task(self, task_id: str) -> None:
        with self._scheduled_lock:
            self._scheduled_task_ids.discard(task_id)
