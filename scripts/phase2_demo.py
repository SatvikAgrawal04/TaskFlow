from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.storage.database import connect
from app.storage.task_repository import TaskRepository
from app.tasks.executor import TaskExecutor
from app.tasks.models import Task
from app.tasks.registry import TaskRegistry


def inspect_database(repo: TaskRepository, label: str) -> None:
    print(f"\n=== {label} ===")
    with connect(repo.database_path) as conn:
        task_rows = conn.execute(
            """
            SELECT task_id, task_type, status, attempt_count, max_attempts,
                   worker_id, next_retry_at, last_error, updated_at
            FROM tasks
            ORDER BY created_at ASC
            """
        ).fetchall()

        print("Tasks:")
        for row in task_rows:
            print(dict(row))

        attempt_rows = conn.execute(
            """
            SELECT task_id, worker_id, attempt_number, started_at, completed_at, outcome, error
            FROM task_attempts
            ORDER BY id ASC
            """
        ).fetchall()

        print("Attempts:")
        for row in attempt_rows:
            print(dict(row))


def main() -> None:
    db_path = Path("data/phase2_demo.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    repo = TaskRepository(db_path)
    registry = TaskRegistry()

    def calculate(task: Task):
        print(f"Handler executing for task {task.task_id}: value={task.payload['value']}")
        return {"result": task.payload["value"] * 2}

    registry.register("calculate", calculate)
    executor = TaskExecutor(registry)

    task = Task.create("calculate", {"value": 21})
    repo.save(task)
    print(f"Created task: {task.task_id} in status {task.status.value}")
    inspect_database(repo, "After task creation")

    print("\nExecuting task through executor...")
    task.mark_running("worker-01")
    result = executor.execute(task, worker_id="worker-01")
    repo.update(task)
    repo.record_attempt(
        task_id=task.task_id,
        worker_id="worker-01",
        attempt_number=task.attempt_count,
        started_at=task.started_at,
        completed_at=task.completed_at,
        outcome="SUCCESS",
        error=None,
    )

    print(f"Result: {result}")
    print(f"Final task status: {task.status.value}")
    inspect_database(repo, "After task completion")
    print(f"\nDatabase persists at: {db_path.resolve()}")


if __name__ == "__main__":
    main()
