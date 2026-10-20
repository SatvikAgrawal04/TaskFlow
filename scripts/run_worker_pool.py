from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from threading import Event

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.queue.worker_pool import WorkerPool
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor
from app.tasks.registry import TaskRegistry


def build_registry() -> TaskRegistry:
    registry = TaskRegistry()

    def calculate(task):
        value = task.payload["value"]
        return {"result": value * 2}

    registry.register("calculate", calculate)
    return registry


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local TaskFlow dispatcher and worker pool.")
    parser.add_argument("--database-path", type=Path, default=Path("data/taskflow.db"))
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--dispatcher-poll-interval", type=float, default=0.5)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    repository = TaskRepository(args.database_path)
    registry = build_registry()
    pool = WorkerPool(
        repository=repository,
        executor=TaskExecutor(registry),
        worker_count=args.workers,
        dispatcher_poll_interval_seconds=args.dispatcher_poll_interval,
    )
    pool.start()
    print(
        f"TaskFlow worker pool is running against {args.database_path.resolve()} "
        f"with {args.workers} workers. Press Ctrl+C to stop."
    )
    try:
        Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        pool.stop()


if __name__ == "__main__":
    main()
