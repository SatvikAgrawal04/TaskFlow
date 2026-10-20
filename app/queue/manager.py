from __future__ import annotations

import logging

from queue import Empty, Full, Queue

from app.logging_utils import log_event

logger = logging.getLogger(__name__)

class WorkQueueFull(Exception):
    pass


class WorkQueueEmpty(Exception):
    pass


class WorkQueue:
    def __init__(self, *, maxsize: int = 1000) -> None:
        if maxsize < 0:
            raise ValueError("maxsize cannot be negative")

        self._queue: Queue[str] = Queue(maxsize=maxsize)
        
    def enqueue(self, task_id: str, *, block: bool = False, timeout: float | None = None) -> None:
        if not task_id:
            raise ValueError("task_id is required")
        
        try:
            self._queue.put(task_id, block=block, timeout=timeout)
            log_event(logger, "task_enqueued", task_id=task_id, queue_depth=self.depth)
            
        except Full as exc:
            raise WorkQueueFull("work queue is full") from exc
    
    
    def dequeue(self, *, block: bool = False, timeout: float | None = None) -> str:
        try:
            task_id = self._queue.get(block=block, timeout=timeout)
            log_event(logger, "task_dequeued", task_id=task_id, queue_depth=self.depth)
            return task_id
        except Empty as exc:
            raise WorkQueueEmpty("work queue is empty") from exc
        
        
    def task_done(self) -> None:
        self._queue.task_done()
        log_event(logger, "queue_task_done", queue_depth=self.depth)
        
    def join(self) -> None:
        self._queue.join()
        
        
    @property
    def depth(self) -> int:
        return self._queue.qsize()
        
        
    