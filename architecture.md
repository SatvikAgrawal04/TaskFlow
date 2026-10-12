# TaskFlow

## Reliable Asynchronous Task Processing System

**Document Type:** Software Architecture & Technical Design Specification  
**Status:** Draft / Pre-Implementation  
**Version:** 0.1  
**Primary Language:** Python 3  
**Deployment Model:** Local / single-machine  
**Cost Constraint:** Zero-cost development and execution  
**Infrastructure Constraint:** No external infrastructure or software requiring administrator privileges

---

# 1. Executive Summary

TaskFlow is a lightweight, reliable asynchronous task processing system built entirely in Python.

It accepts tasks from clients, persists task state, dispatches executable task IDs onto an in-memory queue, and processes that work concurrently using a configurable worker pool. The system is designed to tolerate transient task failures, recover work left unfinished after process failure, and expose enough state and metrics to understand what is happening inside the system.

The project intentionally avoids managed infrastructure such as Redis, RabbitMQ, Kafka, AWS services, or Docker-based dependencies. Python's standard library provides the concurrency, queueing, persistence, logging, and process-control foundations. Where object storage behavior is useful, `boto3` is used against Moto's local mock implementation of Amazon S3.

The objective is not to recreate Celery feature-for-feature. The objective is to understand and implement the fundamental engineering mechanisms behind reliable asynchronous processing:

- producer/consumer concurrency
- worker pools
- persistent task state
- retries and backoff
- crash recovery
- idempotency
- reconciliation
- observability
- graceful shutdown
- bounded work queues
- failure handling

The project should remain small enough that every major design decision can be understood and defended in a technical interview.

---

# 2. Problem Statement

Many backend applications perform operations that should not block the request that initiated them.

Examples include:

- report generation
- file processing
- notifications
- document conversion
- data transformation
- image processing
- batch calculations
- uploads to object storage
- downstream API calls

Executing these operations synchronously creates several problems:

1. The requesting API must remain open while work is performed.
2. Slow work increases response latency.
3. Temporary downstream failures can fail an otherwise successful request.
4. Bursts of incoming work can overwhelm the application.
5. A worker/process failure can leave work unfinished or lost.
6. Without persistent state, operators cannot reliably determine what happened to a task.
7. Reprocessing a task after failure can create duplicate side effects.

TaskFlow addresses these problems by separating **task submission** from **task execution**.

The system provides a durable task record, asynchronous execution through worker threads, retry behavior for recoverable failures, and recovery mechanisms for tasks left in an uncertain state.

---

# 3. Goals

## 3.1 Primary Goals

TaskFlow shall:

1. Accept tasks asynchronously.
2. Assign every task a unique identifier.
3. Persist task metadata and lifecycle state.
4. Process tasks concurrently using a worker pool.
5. Support multiple task types through an extensible task registry.
6. Retry recoverable failures.
7. Apply configurable backoff between retries.
8. Recover tasks abandoned by crashed workers/processes.
9. Prevent or control duplicate execution through idempotency mechanisms.
10. Provide structured logs suitable for debugging task lifecycles.
11. Provide application-level metrics for queue and worker behavior.
12. Gracefully shut down workers without unnecessarily abandoning active work.
13. Support local S3-like object-storage integration through `boto3` + Moto.
14. Be fully runnable without paid infrastructure.
15. Be testable locally and without external services.

## 3.2 Secondary Goals

The project should also:

- provide a clean separation between API, task orchestration, persistence, execution, and storage
- make failure behavior explicit
- make concurrency behavior understandable
- provide a foundation for experimenting with distributed-systems concepts
- be suitable for technical interviews and live architecture discussions

---

# 4. Non-Goals

The initial system is **not** intended to provide:

- production-grade distributed scheduling across many machines
- exactly-once execution guarantees
- multi-node leader election
- a distributed message broker
- multi-tenant authorization
- a full replacement for Celery, Kafka, RabbitMQ, SQS, or similar systems
- production AWS deployment
- automatic horizontal scaling across physical machines
- enterprise authentication and authorization

These may be explored later as learning extensions, but they should not complicate the initial implementation.

---

# 5. Design Constraints

The project is intentionally designed around the following constraints.

## 5.1 Language Constraint

Only Python may be used for application implementation.

## 5.2 Infrastructure Constraint

The project must not depend on infrastructure that:

- requires administrator privileges
- requires paid resources
- requires a continuously running external service

## 5.3 Dependency Constraint

Python standard-library modules should be preferred wherever practical.

Third-party dependencies should be limited to libraries that can be installed into a Python virtual environment without administrator privileges.

## 5.4 Cost Constraint

The system must operate entirely locally and incur no cloud or infrastructure charges.

## 5.5 Learning Constraint

The implementation should favor explicit mechanisms over hiding behavior behind high-level frameworks.

For example:

- use `queue.Queue` to understand producer/consumer behavior
- use `threading` to understand worker coordination
- use SQLite to understand persisted task state
- implement retry/recovery semantics explicitly

Frameworks may be added around the core system after the core behavior is understood.

---

# 6. Proposed Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | Python 3 | Application implementation |
| Concurrency | `threading` | Worker threads and concurrent execution |
| Queue | `queue.Queue` | Thread-safe in-memory work queue |
| Persistence | `sqlite3` | Durable task ledger |
| API | FastAPI | HTTP interface |
| API server | Uvicorn | Local ASGI server |
| Validation | Pydantic | Request/task validation |
| Object storage API | `boto3` | S3 client interface |
| Object storage simulation | Moto | Local/mock S3 behavior |
| Logging | `logging` | Structured application logging |
| Testing | pytest | Unit and integration testing |
| Configuration | environment variables + Python configuration | Runtime configuration |
| Version control | Git | Source control |

## 6.1 Initial Minimal Dependency Set

The first functional version should rely primarily on:

```text
Python standard library
├── threading
├── queue
├── sqlite3
├── logging
├── uuid
├── time
├── datetime
├── dataclasses
├── enum
├── pathlib
└── json
```

Then add:

```text
FastAPI
Uvicorn
Pydantic
boto3
moto
pytest
```

as corresponding features are implemented.

---

# 7. High-Level Architecture

The system follows a producer/consumer architecture with persistent task state.
The authoritative flow is:

```text
API request
   |
   v
SQLite insert: PENDING
   |
   v
Dispatcher scans durable state for executable tasks
   |
   v
In-memory queue receives task_id only
   |
   v
Worker atomically claims task in SQLite
   |
   v
Registered handler executes
   |
   v
SQLite records COMPLETED / RETRY_PENDING / FAILED
```

The API does not directly execute work and should not rely on the in-memory queue for durability. The dispatcher/scheduler is responsible for moving eligible durable tasks into the queue.

```text
                         ┌─────────────────────┐
                         │       Client        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │      FastAPI        │
                         │   Task Submission   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Task Repository   │
                         │      SQLite         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Work Queue       │
                         │    queue.Queue      │
                         └──────────┬──────────┘
                                    │
                ┌───────────────────┼───────────────────┐
                │                   │                   │
                ▼                   ▼                   ▼
        ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
        │  Worker 1   │     │  Worker 2   │     │  Worker N   │
        └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
               │                   │                   │
               └───────────────────┼───────────────────┘
                                   ▼
                         ┌─────────────────────┐
                         │   Task Executor     │
                         └──────────┬──────────┘
                                    │
                       ┌────────────┼────────────┐
                       │            │            │
                       ▼            ▼            ▼
                    Success       Retry        Permanent
                       │            │           Failure
                       ▼            ▼            ▼
                  COMPLETED   Backoff/Pending   FAILED
                       │
                       ▼
              ┌─────────────────────┐
              │ Object Storage      │
              │ boto3 + Moto        │
              └─────────────────────┘

                Recovery / Reconciliation
                         │
                         ▼
              ┌─────────────────────┐
              │ SQLite + Task State │
              │ + Object State      │
              └─────────────────────┘
```

---

# 8. Architectural Principles

## 8.1 Separation of Concerns

The following concerns should remain separate:

- HTTP/API handling
- task persistence
- queue management
- worker lifecycle
- task execution
- retry policy
- recovery/reconciliation
- object storage
- metrics
- configuration

The API should not know how a task is executed.

The worker should not know how an HTTP request was created.

The task repository should not contain business execution logic.

## 8.2 Persistent State Is the Source of Truth

The in-memory queue is a **delivery mechanism**, not the authoritative record of task state.

SQLite is the durable task ledger.

If the process exits unexpectedly, the queue disappears but the task records remain.

## 8.3 Explicit State Transitions

Task lifecycle transitions should be explicit and validated.

Example:

```text
PENDING
   │
   ▼
RUNNING
  │  │
  │  └──────────────► RETRY_PENDING
  │                         │
  │                         ▼
  │                       PENDING
  │
  ├──────────────────────► COMPLETED
  │
  └──────────────────────► FAILED
```

Invalid transitions should be rejected.

## 8.4 At-Least-Once Execution

The initial architecture should favor **at-least-once processing** over attempting to claim exactly-once semantics.

A task may be executed again after a crash or uncertain failure.

Therefore task handlers should be designed to be idempotent where practical.

## 8.5 Dispatcher Owns Scheduling

The API should persist a `PENDING` task and return once the durable record exists.

A dispatcher/scheduler is responsible for scanning SQLite for executable work and offering task IDs to the in-memory queue.

Queue entries are delivery hints, not ownership claims. A worker must still perform the atomic SQLite claim before executing a handler.

---

# 9. Core Components

## 9.1 Task API

Responsible for:

- accepting task submissions
- validating requests
- generating task IDs
- returning task status
- optionally requesting retry/cancellation in later versions

Example endpoints:

```text
POST   /tasks
GET    /tasks/{task_id}
GET    /tasks
GET    /health
GET    /metrics
```

### Example request

```json
{
  "task_type": "upload_file",
  "payload": {
    "source": "example.txt",
    "destination": "results/example.txt"
  }
}
```

### Example response

```json
{
  "task_id": "01J...",
  "status": "PENDING"
}
```

## 9.2 Task Dispatcher / Scheduler

Responsible for:

- scanning SQLite for tasks eligible to run
- placing eligible task IDs onto the in-memory queue
- respecting queue capacity
- allowing retry delays to elapse before retry work is requeued
- avoiding unnecessary duplicate queue entries where practical
- remaining safe if duplicate queue entries still occur

The dispatcher should enqueue only lightweight task references, such as `task_id`. Workers should load the latest task state from SQLite before claiming and executing.

---

# 10. Task Model

A task should contain enough information to understand and recover its lifecycle.

Recommended fields:

```text
task_id
task_type
payload
status
attempt_count
max_attempts
created_at
started_at
completed_at
next_retry_at
last_error
result_key
result_metadata
worker_id
idempotency_key
updated_at
```

Not every field must be implemented in version 1.

Completed tasks should expose where their output went. For simple tasks, `result_key` can point to a local path or object-storage key, while `result_metadata` can store JSON summary data. If tasks later produce multiple outputs, a separate `task_results` table is cleaner than expanding the main task row indefinitely.

---

# 11. Task States

Recommended initial state machine:

| State | Meaning |
|---|---|
| PENDING | Accepted and waiting for execution |
| RUNNING | Claimed by a worker |
| RETRY_PENDING | Failed but eligible for retry after backoff |
| COMPLETED | Successfully executed |
| FAILED | Permanently failed |
| CANCELLED | Cancelled before completion; optional later feature |

### Transition examples

```text
PENDING -> RUNNING
RUNNING -> COMPLETED
RUNNING -> RETRY_PENDING
RETRY_PENDING -> PENDING
RUNNING -> FAILED
PENDING -> CANCELLED
RETRY_PENDING -> CANCELLED
```

---

# 12. Task Queue

The first implementation should use:

```python
queue.Queue
```

This provides thread-safe producer/consumer semantics.

## Dispatcher Responsibilities

- obtain eligible tasks from persistent storage
- enqueue executable task IDs, not full task payloads
- respect the configured queue capacity
- avoid duplicating queue entries unnecessarily
- tolerate duplicate queue entries through worker-side atomic claiming
- poll for new `PENDING` tasks on a fixed interval (e.g. `DISPATCHER_POLL_INTERVAL_SECONDS`, configurable, default ~0.5-1s); this trades submission-to-execution latency against CPU usage from busy-polling, and should be tuned rather than left implicit

## Consumer Responsibilities

- retrieve work
- execute it
- acknowledge completion to the queue
- update persistent task state

The queue should not be treated as durable storage.

## Atomic Task Claiming

Enqueuing a task and marking it `RUNNING` are two different concerns and must not be conflated. The queue only decides *which worker* picks up a task; SQLite must decide *whether a task is still claimable at all*.

The transition from `PENDING` to `RUNNING` must be performed as a single conditional update, not a read followed by a separate write:

```sql
UPDATE tasks
SET status = 'RUNNING',
    worker_id = ?,
    started_at = ?,
    updated_at = ?,
    attempt_count = attempt_count + 1
WHERE task_id = ? AND status = 'PENDING';
```
`attempt_count` is incremented only when the claim succeeds. This makes it count actual execution attempts rather than submissions, queue insertions, or dispatcher observations.

The worker must check the affected row count. If zero rows were updated, another process (e.g. the reconciler, or a duplicate dispatcher pass) already claimed the task, and the worker should discard it rather than execute it. This prevents double-execution in the case where the dispatcher and the reconciliation loop (Section 17) both consider the same task eligible around the same time.

---

# 13. Worker Pool

A configurable number of worker threads will process queued tasks.

Example:

```text
WORKER_COUNT = 4

Queue
 ├── Worker 1
 ├── Worker 2
 ├── Worker 3
 └── Worker 4
```

Each worker should:

1. wait for available work
2. claim a task
3. transition it to `RUNNING`
4. execute the registered handler
5. record success/failure
6. trigger retry logic where appropriate
7. acknowledge queue completion
8. continue until shutdown

Workers should receive a unique worker identifier for observability.

Example:

```text
worker-01
worker-02
worker-03
worker-04
```

---

# 14. Task Registry

Task execution should be decoupled from the worker.

Conceptually:

```python
TASK_REGISTRY = {
    "calculate": calculate_task,
    "upload_file": upload_file_task,
    "generate_report": generate_report,
}
```

The worker asks the registry for the handler associated with the task type.

This avoids hard-coding business logic inside the worker loop.

---

# 15. Retry Strategy

Not every failure should be retried.

Failures should be classified into:

### Retryable

Examples:

- temporary storage failure
- transient network failure
- temporary dependency failure

### Non-retryable

Examples:

- invalid payload
- unknown task type
- malformed input
- permanent validation failure

Recommended retry policy:

```text
attempt 1 → fail
             ↓
           1 sec
             ↓
attempt 2 → fail
             ↓
           2 sec
             ↓
attempt 3 → fail
             ↓
         FAILED
```

This example assumes `max_attempts = 3`, matching the default in the SQLite schema (§19). Keep the retry-policy example and the schema default in sync as `max_attempts` becomes configurable.

Use exponential backoff with configurable limits.

Example formula:

```text
delay = min(base_delay * 2^(attempt - 1), max_delay)
```

Jitter can be added later to avoid synchronized retries.
On a retryable failure, the worker should set:

```text
status = RETRY_PENDING
next_retry_at = now + calculated_delay
last_error = summarized exception
```

The reconciler is then responsible for moving elapsed `RETRY_PENDING` tasks back to `PENDING`.

---

# 16. Crash Recovery

This is a major feature of TaskFlow.

Consider:

```text
Task 42 = RUNNING
Worker executes task
Process crashes
```

The in-memory worker state disappears.

SQLite still contains:

```text
task_id = 42
status = RUNNING
```

At startup, TaskFlow should identify tasks that were left in `RUNNING` beyond a configured timeout.

Those tasks should be considered stale.

Recovery:

```text
RUNNING
   ↓
detect stale task
   ↓
reset/requeue
   ↓
PENDING
   ↓
queue
   ↓
worker
```

The design must avoid immediately declaring every currently running task failed simply because the application restarted.

A lease/heartbeat approach may be introduced later.

---

# 17. Reconciliation

Reconciliation is the mechanism that detects inconsistency between expected and actual system state.

Example:

```text
SQLite:
Task 100 → upload expected

Moto S3:
Object does not exist
```

The reconciler can identify the inconsistency and schedule corrective work.

Possible responsibilities:

- find stale `RUNNING` tasks
- find `RETRY_PENDING` tasks whose retry time has arrived
- detect incomplete uploads
- identify orphaned records
- restore tasks to an executable state
- record reconciliation actions

The reconciler should be safe to run repeatedly.
The minimum implementation should include:

- `RETRY_PENDING` tasks whose `next_retry_at <= now` move back to `PENDING`
- stale `RUNNING` tasks move back to `PENDING`
- each reconciliation action is recorded in logs and metrics

### Reconciliation Cadence

Reconciliation is not a one-time startup step. Startup recovery (§16) handles tasks abandoned by a *previous* process before the current process was even running. But `RETRY_PENDING` tasks becoming eligible, and a worker crashing *while the current process is otherwise healthy*, are ongoing conditions that startup recovery alone will not catch.

The reconciler should therefore run on its own periodic background thread (e.g. every `RECONCILIATION_INTERVAL_SECONDS`, configurable, separate from the dispatcher's poll interval) in addition to the one-time pass performed at startup. Because reconciliation actions (resetting a stale `RUNNING` task to `PENDING`, moving an elapsed `RETRY_PENDING` task back to `PENDING`) go through the same atomic conditional-update pattern described in Section 12, running the reconciler concurrently with normal dispatcher/worker activity is safe.

---

# 18. Idempotency

Because at-least-once execution may result in repeated execution, task operations should be designed to avoid dangerous duplicates.

For example, an upload task should preferably use a deterministic object key:

```text
results/{task_id}/output.json
```

Before creating a side effect, the handler can determine whether the intended result already exists.

### Check-then-act is not sufficient on its own

Checking whether an object exists and then uploading it is itself a race condition: crash recovery (§16) can cause the same logical task to be picked up by two workers in close succession (e.g. one worker still finishing an upload while its `RUNNING` row is concurrently reset by the reconciler due to a too-aggressive stale timeout). Both could pass the "does it exist" check before either has written.

Two practical mitigations, in increasing order of robustness:

1. Rely on the atomic claim in §12 to make double-*starts* rare in the first place, rather than relying solely on the storage-layer check.
2. For operations where a duplicate side effect would be costly, use an idempotency ledger: a table (or unique constraint) keyed on `idempotency_key` that records whether this logical operation is already in progress or already completed. A duplicate attempt should fail a uniqueness check or observe an existing completed record instead of silently double-executing.

Idempotency should be treated as a design principle rather than a guarantee that every arbitrary task can be executed twice safely.
Idempotency should also be defined per task type. Generating a deterministic report file, uploading an object, sending an email, and calling a payment-like downstream API have different duplicate-risk profiles.

For SQLite-only side effects, an idempotency ledger can be updated in the same database transaction as task completion. For external side effects such as S3 uploads, SQLite and the object store cannot share a true transaction, so handlers should use deterministic output keys and reconciliation should handle partial states such as "object exists, but task did not reach `COMPLETED`."

---

# 19. SQLite Task Ledger

SQLite serves as the durable coordination record.

Example schema:

```sql
CREATE TABLE tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    idempotency_key TEXT,
    worker_id TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    next_retry_at TEXT,
    last_error TEXT,
    result_key TEXT,
    result_metadata TEXT,
    updated_at TEXT NOT NULL
);
```

Recommended indexes:

```sql
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_retry ON tasks(status, next_retry_at);
CREATE INDEX idx_tasks_running ON tasks(status, started_at);
CREATE INDEX idx_tasks_updated_at ON tasks(updated_at);
```

These indexes support dispatcher scans, retry eligibility checks, stale-running recovery, and operational queries.

A separate attempt/history table is recommended once the basic implementation is stable:

```sql
CREATE TABLE task_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    worker_id TEXT,
    attempt_number INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT NOT NULL,
    error TEXT
);
```

This provides an audit trail of execution attempts.

---

# 20. SQLite Concurrency Considerations

SQLite is intentionally used because it requires no external server.

However, it introduces important architectural constraints:

- database connections should not be shared carelessly between threads
- writes should be short and controlled
- transactions must be explicit
- contention should be minimized
- SQLite should not be treated as a distributed database

The database layer should hide connection management from the rest of the application.

A practical approach is to use a connection per operation/thread rather than sharing a single connection globally.

### Required PRAGMA settings

By default, SQLite uses a rollback journal, which serializes writers and readers against each other and will raise `database is locked` errors quickly under concurrent worker threads. Every connection should set, at minimum:

```python
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA busy_timeout=5000;")
```

- `journal_mode=WAL` allows readers to proceed concurrently with a single writer, which matters once multiple worker threads and the reconciler are all touching the database.
- `busy_timeout` makes SQLite retry internally for up to the given number of milliseconds instead of immediately raising `sqlite3.OperationalError` on lock contention, which is otherwise the most common source of flaky behavior under load.

These should be set once per connection, not once globally, since WAL mode is a per-connection/per-database-file setting.

---

# 21. Object Storage Simulation

TaskFlow can include a task type that writes output files to S3.

The application uses:

```text
boto3
```

while Moto provides a local/mock implementation.

Architecture:

```text
Task Handler
     ↓
boto3 S3 Client
     ↓
Moto
     ↓
Local Mock S3
```

The application should depend on the S3 API rather than Moto-specific behavior.

This allows the storage implementation to later be replaced with real AWS S3 without redesigning the task execution layer.

### Important limitation

Moto is a mock implementation. It does not reproduce every operational characteristic of Amazon S3.

Therefore tests should distinguish between:

- application behavior tested with Moto
- actual AWS service behavior, which is outside the project's zero-cost local environment

---

# 22. Observability

Observability should be designed before implementation rather than added after failures occur.

## 22.1 Logging

Logs should contain enough information to trace a task through its lifecycle.

At minimum:

```text
timestamp
level
task_id
task_type
worker_id
attempt_number
event
duration
error
```

Example concept:

```text
2026-09-02T20:15:12 INFO
task_id=abc123
worker_id=worker-02
event=task_started
attempt=1
```

Avoid logging sensitive payload contents unnecessarily.

## 22.2 Metrics

Initial metrics:

```text
tasks_submitted_total
tasks_completed_total
tasks_failed_total
tasks_retried_total
active_workers
queue_depth
task_execution_duration
task_wait_duration
```

Later:

```text
retry_rate
failure_rate
stale_tasks
reconciliation_count
storage_upload_failures
```

A custom in-process metrics collector is sufficient initially.

No Prometheus/Grafana dependency is required.

---

# 23. Health Checks

At minimum:

```text
GET /health
```

should indicate whether the process is alive.

A later readiness check can distinguish:

```text
Liveness:
Is the application process running?

Readiness:
Can it accept/process tasks?
```

Example conceptual response:

```json
{
  "status": "healthy",
  "workers": 4,
  "queue_depth": 3
}
```

---

# 24. Graceful Shutdown

The process should support controlled shutdown.

Expected behavior:

```text
shutdown signal
      ↓
stop accepting new work
      ↓
signal workers to stop after current work
      ↓
finish or safely release active tasks
      ↓
persist final task state
      ↓
exit
```

A forced crash is different from graceful shutdown and must be handled by startup recovery.

---

# 25. Failure Scenarios

The architecture should explicitly consider failure before implementation.

## Scenario A — Task handler fails

```text
RUNNING
   ↓
exception
   ↓
retryable?
 ┌─┴────────┐
yes         no
 │           │
 ▼           ▼
retry      FAILED
```

## Scenario B — Worker crashes

```text
RUNNING
   ↓
worker/process dies
   ↓
task remains RUNNING
   ↓
startup reconciliation
   ↓
PENDING
   ↓
requeue
```

## Scenario C — Queue grows faster than workers

```text
producer rate > consumer rate
          ↓
queue depth increases
          ↓
backpressure/capacity policy needed
```

The first version should make queue capacity configurable.

## Scenario D — Storage upload fails

```text
upload
  ↓
failure
  ↓
retryable?
  ↓
retry with backoff
```

## Scenario E — Application restarts

```text
SQLite persists
queue disappears
        ↓
startup recovery
        ↓
repopulate queue from PENDING/recoverable tasks
```

## Scenario F — Duplicate execution

```text
retry/recovery
      ↓
same task executes again
      ↓
idempotency protection
```

---

# 26. Backpressure

An unbounded queue can allow memory usage to grow indefinitely if producers consistently outpace consumers.

Therefore the architecture should consider a bounded queue.

Example:

```python
queue.Queue(maxsize=1000)
```

When capacity is exhausted, the system must have an explicit policy.

Possible policies:

- reject new submissions
- block producers briefly
- persist tasks but delay in-memory enqueueing
- return HTTP 429/503
- prioritize certain tasks

The first version can choose a simple policy, but the decision must be documented.
Recommended initial policy:

- accept a task only after it is durably inserted into SQLite
- return success from the API after durable insertion, not after in-memory enqueueing
- if the queue is full, leave the task in `PENDING`
- allow the dispatcher to enqueue it on a later pass when capacity is available
- expose queue depth and pending backlog through metrics

This keeps accepted work durable while still bounding in-memory growth. A later version may add an admission limit such as `TASKFLOW_MAX_PENDING_TASKS` and return HTTP `429` or `503` when the durable backlog is too large.

---

# 27. API Design

## POST /tasks

Submit a new task.

### Request

```json
{
  "task_type": "calculate",
  "payload": {
    "value": 42
  }
}
```

### Response

```json
{
  "task_id": "abc123",
  "status": "PENDING"
}
```

## GET /tasks/{task_id}

Returns current task state.

Example:

```json
{
  "task_id": "abc123",
  "task_type": "calculate",
  "status": "COMPLETED",
  "attempt_count": 1
}
```

## GET /health

Returns service health.

## GET /metrics

Returns system/application metrics.

Additional endpoints should be added only when there is a clear requirement.

## Runtime Lifecycle Note

Background components should be started through the FastAPI lifespan/startup mechanism, not at module import time.

The initial architecture assumes one Python process owns the in-memory queue, dispatcher, reconciler, and worker pool. Uvicorn should therefore run with a single worker process for this version. Running with multiple Uvicorn workers would create multiple independent in-memory queues and worker pools over the same SQLite database, which is outside the initial design.

---

# 28. Configuration

Configuration should not be hard-coded.

Example environment variables:

```text
TASKFLOW_WORKER_COUNT=4
TASKFLOW_QUEUE_SIZE=1000
TASKFLOW_DISPATCHER_POLL_INTERVAL_SECONDS=0.5
TASKFLOW_RECONCILIATION_INTERVAL_SECONDS=5
TASKFLOW_MAX_ATTEMPTS=3
TASKFLOW_RETRY_BASE_DELAY=1
TASKFLOW_RETRY_MAX_DELAY=60
TASKFLOW_STALE_TASK_TIMEOUT=300
TASKFLOW_MAX_PENDING_TASKS=
TASKFLOW_DATABASE_PATH=./data/taskflow.db
TASKFLOW_LOG_LEVEL=INFO
```

A typed configuration object should expose these values internally.

---

# 29. Security Requirements

This is a learning project, but basic secure engineering practices should still be followed.

## Required

- validate API input
- avoid unsafe dynamic execution
- do not use `eval()` for task execution
- restrict registered task types
- avoid secrets in source control
- avoid logging sensitive payloads
- validate object keys and file paths
- use environment variables for credentials if ever required
- use parameterized SQL
- validate external input before persistence

## Not required initially

- OAuth
- JWT authentication
- RBAC
- TLS termination
- enterprise secrets management

Adding security features without an actual requirement should not distract from the reliability architecture.

---

# 30. Testing Strategy

Testing should be designed around behavior and failure modes.

## 30.1 Unit Tests

Test:

- task state transitions
- retry classification
- retry delay calculation
- task validation
- idempotency behavior
- repository operations
- registry behavior
- metrics calculation

## 30.2 Integration Tests

Test:

- task submission → persistence → queue → execution
- failure → retry → success
- failure → max retries → FAILED
- worker processing multiple tasks
- SQLite + worker interaction
- boto3 + Moto upload flow
- recovery after simulated abandoned task

## 30.3 Failure Tests

Intentionally simulate:

- task exceptions
- storage failures
- worker failure
- stale `RUNNING` tasks
- invalid task types
- malformed payloads
- queue saturation

## 30.4 Concurrency Tests

Validate:

- multiple workers can process tasks concurrently
- no task is unintentionally executed twice under normal operation
- SQLite state remains consistent
- queue accounting remains correct

Concurrency tests may be timing-sensitive and should avoid relying on arbitrary sleeps wherever possible.

---

# 31. Performance Measurements

The project should measure, rather than merely claim, system behavior.

Useful measurements:

### Throughput

```text
tasks completed / second
```

### Queue wait time

```text
task dequeue time - task enqueue time
```

### Execution time

```text
task completion time - task start time
```

### End-to-end latency

```text
task completion time - task submission time
```

### Retry rate

```text
retried tasks / total tasks
```

### Failure rate

```text
failed tasks / total tasks
```

The project should include a simple benchmark script capable of generating a configurable number of tasks.

---

# 32. Recommended Repository Structure

Start simple, then introduce modules as complexity grows.

```text
taskflow/
│
├── app/
│   ├── api/
│   │   └── routes.py
│   │
│   ├── queue/
│   │   ├── manager.py
│   │   └── worker.py
│   │
│   ├── tasks/
│   │   ├── executor.py
│   │   └── registry.py
│   │
│   ├── storage/
│   │   ├── database.py
│   │   └── object_store.py
│   │
│   ├── recovery/
│   │   └── reconciler.py
│   │
│   ├── metrics/
│   │   └── collector.py
│   │
│   ├── config.py
│   └── main.py
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── scripts/
│   ├── benchmark.py
│   └── simulate_failures.py
│
├── data/
│   └── .gitkeep
│
├── README.md
├── requirements.txt
└── .gitignore
```

Do not create every module immediately. The architecture should evolve with actual requirements.

---

# 33. Development Approach

The project should be built incrementally.

## Phase 0 — Architecture First

Before writing application code, finalize:

- problem statement
- goals/non-goals
- task lifecycle
- architecture diagram
- failure model
- persistence model
- retry semantics
- worker lifecycle
- configuration
- testing strategy

Deliverable:

```text
architecture.md
```

## Phase 1 — Core Task Engine

Implement:

- task model
- task registry
- worker
- queue
- dispatcher/queue handoff
- basic execution
- logging

No FastAPI yet.

Goal:

```text
create task
→ queue
→ worker
→ execute
→ completed
```

## Phase 2 — Persistence

Add SQLite.

Implement:

- task repository
- task state transitions
- attempt tracking

Goal:

```text
task state survives process restart
```

## Phase 3 — Failure Handling

Add:

- retry classification
- max attempts
- exponential backoff
- failure recording
- `next_retry_at` persistence
- elapsed retry reconciliation

Goal:

```text
failed task ≠ lost task
```

## Phase 4 — Crash Recovery

Add:

- stale task detection
- startup recovery
- requeueing
- periodic reconciler for stale `RUNNING` work

Goal:

```text
process crash
→ recover abandoned work
```

## Phase 5 — Object Storage

Add:

- boto3
- Moto
- upload task
- object existence checks

Goal:

```text
worker
→ boto3
→ Moto S3
```

## Phase 6 — API

Add:

- FastAPI
- task submission endpoint
- status endpoint
- health endpoint

Goal:

```text
HTTP client
→ TaskFlow
→ asynchronous execution
```

## Phase 7 — Observability

Add:

- structured task logs
- metrics
- queue depth
- worker state
- task latency
- retry counters

Goal:

```text
answer:
"What is the system doing right now?"
```

## Phase 8 — Reliability Hardening

Add:

- idempotency
- bounded queue
- graceful shutdown
- lease/heartbeat hardening
- stronger concurrency tests
- benchmark tooling

Goal:

```text
system behaves predictably under failure and load
```

---

# 34. Architecture Decision Records

Important design decisions should be captured as ADRs.

Recommended initial ADRs:

### ADR-001: Use SQLite for persistent task state

Document:

- why SQLite
- alternatives considered
- benefits
- limitations

### ADR-002: Use Python threads for worker concurrency

Document:

- why threads
- expected workload
- limitations caused by Python's GIL
- why processes are not used initially

### ADR-003: Treat SQLite as the source of truth

Document:

- why an in-memory queue is insufficient
- recovery implications

### ADR-004: Use at-least-once execution semantics

Document:

- why exactly-once is not promised
- duplicate execution implications
- idempotency strategy

### ADR-005: Use Moto for local object-storage testing

Document:

- zero-cost requirement
- application/S3 abstraction
- mock limitations

---

# 35. Important Technical Questions to Answer Before Coding

An architect should be able to answer these before implementation.

## Task Lifecycle

- What exactly constitutes task creation?
- When does a task become `RUNNING`?
- Who is allowed to change task state?
- What happens when state updates fail?

## Queue

- Is the queue bounded?
- What happens when it is full?
- Who inserts work into it?
- How is queue state rebuilt after restart?

## Workers

- How many workers exist?
- How are workers stopped?
- What happens to work currently being processed?

## Reliability

- Which errors are retryable?
- How many attempts are permitted?
- How does backoff work?
- What happens after the final attempt?

## Recovery

- How do we know a task is stale?
- What happens to a task after process crash?
- How do we avoid duplicate side effects?

## Persistence

- What is stored in SQLite?
- What is the authoritative state?
- How do concurrent writes behave?

## Scalability

- What happens if task arrival doubles?
- What happens if workers cannot keep up?
- What is the bottleneck?
- What changes would be required to move beyond a single process?

## Observability

- How do we trace one task?
- How do we detect stuck work?
- How do we know the queue is overloaded?
- How do we measure worker utilization?

These questions should directly influence implementation.

---

# 36. Future Extensions

After the core system is stable, possible extensions include:

## 36.1 Multi-process workers

Replace or supplement threads with Python processes.

This allows experimentation with CPU-bound workload behavior and process isolation.

## 36.2 Priority queues

Support:

```text
HIGH
NORMAL
LOW
```

priority task execution.

## 36.3 Scheduled tasks

Support:

```text
run_at = timestamp
```

and execute tasks only when eligible.

## 36.4 Dead-letter queue

Keep permanently failed tasks in a separate logical queue for inspection/reprocessing.

## 36.5 Task cancellation

Introduce cancellation semantics and worker cooperation.

## 36.6 Heartbeats / leases

Workers periodically renew ownership of tasks.

This provides a more robust alternative to simple stale-timeout recovery.

## 36.7 Multiple queue types

For example:

```text
default
slow
high-priority
storage
```

## 36.8 Distributed version

A future experiment could replace the local queue/database with a real broker/database, but that is explicitly outside the current cost and infrastructure constraints.

---

# 37. Expected Final Capabilities

The final local system should demonstrate:

```text
✓ asynchronous task submission
✓ configurable worker pool
✓ thread-safe queue
✓ persistent task ledger
✓ explicit task state machine
✓ retryable vs permanent failure
✓ exponential backoff
✓ crash recovery
✓ reconciliation
✓ idempotent task design
✓ simulated S3 object storage
✓ structured logging
✓ health endpoint
✓ application metrics
✓ graceful shutdown
✓ unit/integration/failure tests
✓ benchmark tooling
```

---

# 38. Success Criteria

The project is considered successful when it can demonstrate the following scenarios from a clean local setup.

### Scenario 1 — Normal Processing

Submit 100 tasks.

Expected:

```text
100 submitted
100 completed
0 lost
```

### Scenario 2 — Temporary Failure

Inject failures into some tasks.

Expected:

```text
task fails
→ retry
→ succeeds
```

### Scenario 3 — Permanent Failure

Force a task to fail on every attempt.

Expected:

```text
attempt 1
attempt 2
attempt 3
→ FAILED
```

### Scenario 4 — Process Crash

Allow a worker/task to enter `RUNNING`, terminate the process, and restart it.

Expected:

```text
abandoned task detected
→ recovered
→ executed again
```

### Scenario 5 — Storage Failure

Make a simulated S3 operation fail.

Expected:

```text
upload fails
→ retry/backoff
→ successful upload
```

### Scenario 6 — Duplicate Execution

Force a retry/recovery path.

Expected:

```text
same logical task
→ repeated execution attempt
→ no unintended duplicate final result
```

### Scenario 7 — Queue Pressure

Generate tasks faster than the workers can consume them.

Expected:

```text
queue depth increases
→ system remains stable
→ work eventually drains
```

---

# 39. Engineering Trade-offs

The project intentionally makes several trade-offs.

| Decision | Benefit | Cost |
|---|---|---|
| SQLite | Zero infrastructure, persistent state | Single-machine limitation |
| `queue.Queue` | Simple, thread-safe | Not durable/distributed |
| Threads | Easy concurrency for I/O-heavy tasks | Limited CPU parallelism due to GIL |
| Moto | Free local S3 simulation | Not identical to AWS |
| At-least-once semantics | Practical recovery | Requires idempotency |
| Python-only | Accessible and constraint-compliant | Less representative of multi-language production stacks |
| Single process | Easy to run and understand | Limited horizontal scalability |

The purpose of these trade-offs is educational and architectural clarity, not pretending the system is equivalent to a production distributed task platform.

---

# 40. Resume Positioning

The project should only be added to the resume after it has a meaningful implementation and evidence of engineering depth.

Potential positioning:

> **TaskFlow — Reliable Asynchronous Task Processing System**  
> Built a Python-based asynchronous task processing system with a persistent SQLite task ledger, configurable worker pool, retry/backoff mechanisms, crash recovery, idempotent task execution, and local S3 simulation using boto3 and Moto.

The final wording should be based on what is actually implemented and measured.

---

# 41. Definition of Done

The project should not be considered complete merely because tasks execute successfully.

A meaningful definition of done is:

- [ ] Architecture documented
- [ ] Task state machine documented
- [ ] SQLite schema documented
- [ ] Result/output recording implemented
- [ ] Worker lifecycle implemented
- [ ] Queue behavior implemented
- [ ] Dispatcher/scheduler implemented
- [ ] Retry policy implemented
- [ ] Crash recovery implemented
- [ ] Reconciliation implemented
- [ ] Idempotency strategy implemented
- [ ] Moto S3 integration implemented
- [ ] Structured logs implemented
- [ ] Metrics implemented
- [ ] Health endpoint implemented
- [ ] Graceful shutdown implemented
- [ ] Unit tests implemented
- [ ] Integration tests implemented
- [ ] Failure injection tests implemented
- [ ] Benchmark executed
- [ ] README includes architecture diagram and setup instructions
- [ ] Known limitations documented
- [ ] Architecture decisions captured

---

# 42. Final Architectural Summary

TaskFlow is fundamentally built around one principle:

> **Persist work before processing it, execute it asynchronously, and make failure recoverable.**

The core design is intentionally simple:

```text
                 SUBMIT
                    │
                    ▼
             ┌─────────────┐
             │   SQLite    │
             │ Task Ledger │
             └──────┬──────┘
                    │
                    ▼
             ┌─────────────┐
             │    Queue    │
             └──────┬──────┘
                    │
             ┌──────┴──────┐
             │   Workers   │
             └──────┬──────┘
                    │
                    ▼
                EXECUTE
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
       SUCCESS              FAILURE
          │                   │
          ▼                   ▼
      COMPLETED          RETRY / FAILED
                              │
                              ▼
                         RECOVERY
```

The project's value lies less in the amount of code and more in the engineering decisions around:

- concurrency
- state
- reliability
- failure
- recovery
- idempotency
- observability
- backpressure

That is the foundation on which more sophisticated asynchronous and distributed systems are built.
