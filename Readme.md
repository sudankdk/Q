Q — Queue with WAL, DLQ and Worker Reaper

Project Overview

`Q` is a lightweight Go project implementing a persistent in-memory queue with a write-ahead log (WAL), a dead-letter queue (DLQ), and a worker reaper that cleans up stalled work. It is organized for clarity and testability and designed as a foundation for building reliable message-processing systems.

What I've implemented

- Queue data structures and operations (`internal/queue`)
- Message representation and state management
- Dead-letter queue handling (`internal/queue/dlq.go`)
- Write-ahead log for durability (`wal/wal.go`, `wal/recovery.go`)
- Worker reaper to detect and requeue or DLQ timed-out messages (`worker/reaper.go`)
- Basic test harness (`test_main_test.go`) and command-line entrypoint (`cmd/main.go`)

Architecture

- Producers enqueue messages to the `queue` API.
- The `queue` persists operations to the WAL before acknowledging (durability).
- Consumers (workers) pull messages, process them, and ACK or NACK them.
- If a message times out or exceeds retry policy, it's moved to the DLQ.
- The `worker/reaper` periodically scans in-flight messages to detect stalled work and either requeue or send messages to the DLQ.

Key components:

- `internal/queue`
  - `queue.go` : core queue operations (enqueue, dequeue, ack, nack)
  - `message.go` : message structure and metadata
  - `state.go` : in-memory state and transitions
  - `dlq.go` : dead-letter queue logic and retention
- `wal`
  - `wal.go` : write-ahead log implementation (append, flush)
  - `recovery.go` : replay logic to rebuild in-memory state from WAL on startup
- `worker`
  - `reaper.go` : background process to find/stabilize stuck in-flight messages
- `cmd/main.go` : simple CLI entrypoint used for manual runs or demos

Data Architecture

- Message format: each message contains an `id`, `payload` (opaque bytes or JSON), `headers` (optional map), `created_at` (timestamp), `attempts` (retry count), `status` (READY, IN_FLIGHT, ACKED, DLQ), and `lease_expiry` (timestamp when in-flight lease expires).
- WAL entry format: append-only records representing operations (ENQUEUE, ACK, NACK, REQUEUE, DLQ). Each entry records the operation type, `message_id`, relevant metadata or payload, and a timestamp. Entries are serialized (JSON or binary) and flushed to disk before in-memory state is updated.
- In-memory structures: the runtime keeps a FIFO queue of message IDs, a `messages` map (id -> Message) holding payloads/metadata, and an `in_flight` map tracking leased messages and expiry. The DLQ is represented as a separate persistent list of failed message records.
- Storage layout: the `data/` directory holds WAL segment files (e.g. `wal-0001.log`), optional snapshots for fast recovery, and DLQ files. On startup the WAL is replayed (see `wal/recovery.go`) to rebuild `messages`, queue order, and in-flight leases.
- Retention & compaction: a periodic compaction/snapshot creates a checkpoint of current in-memory state and truncates consumed WAL segments to bound disk usage.
- Consistency: the system enforces WAL-before-ack semantics (write & fsync WAL entry, then update in-memory state) so acknowledged messages are durable; replays guarantee at-least-once delivery semantics and the DLQ captures messages that exceed retry policies.

Flow summary:
1. Enqueue writes to WAL, then updates in-memory queue.
2. Worker dequeues and marks message `IN_FLIGHT`.
3. Worker ACK -> message removed; Worker NACK or timeout -> DLQ or requeue based on policy.
4. Reaper runs periodically to handle stuck `IN_FLIGHT` messages.

Directory layout

Top-level:

- `cmd/` — CLI entrypoint (cmd/main.go)
- `internal/queue/` — queue implementation (internal/queue)
- `wal/` — write-ahead log (wal/wal.go, wal/recovery.go)
- `worker/` — background worker utilities (worker/reaper.go)
- `data/` — runtime data (pluggable storage location)
- `test_main_test.go` — test harness

How to build and run

Requirements: Go 1.20+ (or current stable Go).

Build:
```
go build ./...
```

Run tests:
```
go test ./...
```

Quick run (example):
```
go run ./cmd
```

Notes on what I did (work log)

- Implemented the queue primitives and message lifecycle handling.
- Added WAL persistence and startup recovery to maintain durability.
- Built DLQ semantics for messages that fail or time out.
- Implemented a reaper to reclaim or DLQ in-flight messages after a timeout.
- Added basic tests to exercise the main flows.

