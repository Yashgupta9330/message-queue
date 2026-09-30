The DLQ reuses the existing queue infrastructure — there's no special DLQ type. Here's the full flow:

**Where the decision is made — `ackmgr.go:nackEntry`**

```go
func (a *AckManager) nackEntry(entry inFlightEntry) error {
    a.registry.DecrementInFlight(entry.consumerID)

    if entry.message.RetryCount >= a.maxRetries {
        dlqMsg := entry.message
        dlqMsg.Queue = entry.message.Queue + ".dlq"   // "orders" → "orders.dlq"
        dlqMsg.Status = core.StatusDead
        return a.manager.Enqueue(dlqMsg)              // enqueue to a different queue name
    }
    return a.manager.Enqueue(entry.message.WithRetry())
}
```

**What `manager.Enqueue("orders.dlq", ...)` does:**

The queue.Manager lazily creates a queue on first use. Enqueuing to `"orders.dlq"`:
1. Calls `initQueue("orders.dlq")` — creates `data/wal/orders.dlq.wal` on disk
2. Writes the message record to that WAL file
3. Adds the message to the in-memory `"orders.dlq"` topic

**Why it survives restart:**

On startup, `queue.NewManager` scans the WAL directory for `*.wal` files. It finds both `orders.wal` and `orders.dlq.wal`, replays both, and pre-loads each into its own in-memory topic. The DLQ is just another named queue.

**Two paths that trigger `nackEntry`:**

| Path          | Trigger                          | Code                             |
| ------------- | -------------------------------- | -------------------------------- |
| Explicit NACK | Consumer sends `AckOutcome_NACK` | `AckManager.Nack(id)`            |
| Timeout       | Message in-flight too long       | `AckManager.ScanAndTimeout(...)` |

Both call `nackEntry` — they behave identically from the DLQ's perspective.

**The retry counter** (`RetryCount`) lives on the `Message` struct and is incremented by `WithRetry()` on each re-enqueue. It's also written into the WAL record (`"r"` field in `recordMsg`), so it's preserved across restarts. When the count reaches `maxRetries`, the next failure routes to DLQ instead of requeueing.

**DLQ TTL (30 days default)**

When a message is dead-lettered, `EnqueueTime` is reset to the dead-letter time (used as the TTL clock). A background scanner (`internal/dlqttl`) runs every `DLQ_TTL_SCAN_INTERVAL` and tombstones DLQ messages older than `DLQ_TTL` (default `720h`). This applies only to queues ending in `.dlq` — main queues are never expired. Set `DLQ_TTL=0` to disable.