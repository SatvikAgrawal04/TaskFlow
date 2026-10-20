from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.storage.task_repository import TaskRepository
from app.tasks.service import TaskSubmissionService


def parse_payload(parts: list[str]) -> dict:
    raw_payload = " ".join(parts)
    if raw_payload.startswith("'") and raw_payload.endswith("'"):
        raw_payload = raw_payload[1:-1]
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as original_error:
        # Command Prompt consumes the double quotes in '{"key": value}', leaving
        # {key: value}. Restore bare identifier keys for this common CLI form.
        normalized_payload = re.sub(
            r"([,{]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)",
            r'\1"\2"\3',
            raw_payload,
        )
        if normalized_payload == raw_payload:
            raise original_error
        payload = json.loads(normalized_payload)
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist a task for a running TaskFlow worker pool.")
    parser.add_argument("task_type")
    parser.add_argument("payload", nargs="+", help="JSON object passed to the registered task handler")
    parser.add_argument("--database-path", type=Path, default=Path("data/taskflow.db"))
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--idempotency-key")
    args = parser.parse_args()

    payload = parse_payload(args.payload)

    service = TaskSubmissionService(TaskRepository(args.database_path))
    task = service.submit(
        args.task_type,
        payload,
        max_attempts=args.max_attempts,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps({"task_id": task.task_id, "status": task.status.value}))


if __name__ == "__main__":
    main()
