The WAL is append-only — it only ever grows. Every message write and every ACK tombstone adds bytes to the file. The compactor is what trims it back down.

**The problem it solves**

Imagine a queue that processes 1 million messages per day. Every message gets two WAL records: one when it's enqueued, one tombstone when it's ACKed. After a week, the WAL has 14 million records — but at any given moment, only a few thousand are actually live (un-ACKed). The rest is dead weight that only slows down WAL replay on restart.

**What it does**

```
Before compact:           After compact:
┌─────────────┐           ┌─────────────┐
│ msg-1       │           │ msg-1       │  ← kept (not tombstoned)
│ msg-2       │           │ msg-3       │  ← kept
│ msg-3       │           └─────────────┘
│ tombstone-2 │           ← gone (msg-2 and its tombstone removed)
│ tombstone-1 │
└─────────────┘
```

It replays the WAL (applying tombstones to filter out ACKed messages), writes only the survivors to a temp file, verifies the temp file, then atomically renames it over the original.

**Why atomic (temp file + rename)**

If you rewrote the WAL in place and crashed mid-write, you'd have a partially written file with no way to recover. With the temp file approach:
- Crash before rename → original WAL is untouched, temp file is just deleted
- Crash after rename → compacted WAL is complete and valid
- There's no window where the data is at risk

Without compaction, the WAL grows without bound and restart time grows proportionally (it has to replay every record ever written).

**How to call it**

Compaction is invoked from Go, not a separate binary.

- **Per queue** — if you have a `*queue.Manager` (for example from `queue.NewManager` in the broker), call `Compact(queueName)`. The queue must already exist in the manager (a WAL was opened for it); if the name is unknown, `Compact` returns `nil` (no-op).
- **All queues** — `CompactAll()` compacts every queue that currently has a WAL writer.

The underlying implementation is `(*wal.Writer).Compact()` in `internal/wal/compactor.go`: replay the WAL, write survivors to a `.tmp` file, verify, then rename over the original WAL.