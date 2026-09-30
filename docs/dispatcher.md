---

## `core/message.go` — What is a Message?

This file defines the single most important data structure in the entire broker — the `Message`. Every piece of data that flows through the system is a `Message`. Think of it as a parcel moving through a postal system: it starts somewhere, gets labelled, travels through sorting, gets delivered, and is either confirmed or returned.

**The struct fields:**

```go
ID          MessageID   // unique ID the broker generates when the producer publishes
Queue       string      // which queue this message belongs to ("orders", "payments", etc.)
Payload     []byte      // the actual content — whatever bytes the producer sent
Headers     []Header    // optional key-value metadata, like HTTP headers
RetryCount  int32       // how many times this message has been tried and failed
EnqueueTime time.Time   // when it first arrived at the broker
DispatchedAt time.Time  // when the dispatcher last sent it to a consumer
Status      DeliveryStatus // where in its lifecycle it currently is
```

**`DeliveryStatus`** tracks the message's lifecycle. A message moves through these states in order:

```
Pending  →  InFlight  →  Acked
                    ↘  Nacked  →  (back to Pending, RetryCount++)
                    ↘  Dead    (if retries exhausted)
```

- `StatusPending` — sitting in the queue, waiting to be dispatched
- `StatusInFlight` — sent to a consumer, waiting for an ACK or NACK
- `StatusAcked` — consumer confirmed it processed it, done
- `StatusNacked` — consumer rejected it, goes back to the queue
- `StatusDead` — too many failed retries, dropped

**`AckOutcome`** is what the consumer sends back: `OutcomeAck` (processed successfully) or `OutcomeNack` (failed, try again).

**`WithRetry()`** — when a message is NACKed, it doesn't go back as-is. `WithRetry()` returns a copy with `RetryCount++` and status reset to `Pending`. The original is unchanged — Go structs are values, not references.

**Why `DispatchedAt`?** — (timeout scanner) will periodically scan all in-flight messages and check how long ago they were dispatched. If a consumer received a message but never sent an ACK (crashed, hung), `DispatchedAt` tells the scanner when to give up and re-enqueue it.

---

## `dispatcher/dispatcher.go` — The Traffic Controller

Before the dispatcher existed, nobody was actually moving messages from the queue to the consumers. The queue just held messages and the consumer's `Send` channel sat empty. The dispatcher is the middleman — it runs in the background, picks up messages, and decides which consumer gets each one.

**The struct:**

```go
type Dispatcher struct {
    queueManager *queue.Manager    // where to pull messages from
    registry     *connmgr.Registry // who is connected and who has room
}
```

It holds references to two things built in earlier phases. It owns nothing itself — it's purely a loop that connects them.

---

### `Run(ctx, queueName)` — The main loop

This is where everything happens. You start one of these per queue:

```go
go dispatcher.Run(ctx, "orders")
go dispatcher.Run(ctx, "payments")
```

Each `Run` call is an infinite loop with two nested layers:

**Outer loop — waiting for a message:**

```go
msg, ok := d.queueManager.Dequeue(queueName)
```

This blocks. The goroutine sleeps here doing nothing until a message arrives. When one does, it wakes up and moves to the inner loop.

**Inner loop — finding someone to give it to:**

Now the dispatcher is holding a message and needs to find a consumer who can take it. Three things can happen:

1. **No eligible consumers** — all connected consumers are at their prefetch limit, or nobody is connected at all. The dispatcher waits 5ms and checks again. The message stays held in a local variable — it was already dequeued, so it's safe.

2. **Eligible consumers found** — calls `leastInFlight()` to pick the best one, stamps the message with the current time and `StatusInFlight`, increments that consumer's in-flight counter, then drops it into `consumer.Send`.

3. **Context cancelled** — the server is shutting down. If this happens after the in-flight counter was already incremented (but before the message was actually sent), the increment is undone so the counter stays accurate.

Once the message is sent, `break` exits the inner loop and the outer loop goes back to blocking on `Dequeue`.

---

### `leastInFlight()` — Load balancing

```go
func leastInFlight(consumers []*connmgr.Consumer) *connmgr.Consumer {
    sort.Slice(consumers, func(i, j int) bool {
        return consumers[i].InFlight() < consumers[j].InFlight()
    })
    return consumers[0]
}
```

When multiple consumers are eligible, the dispatcher doesn't pick randomly or round-robin. It picks whoever has the fewest unacknowledged messages right now. This naturally routes more messages to fast consumers (who ACK quickly and keep their count low) and fewer to slow ones (whose count stays high). No configuration needed — the load balances itself.

---

### The full flow connecting all phases

```
Producer publishes
    ↓
WAL write + in-memory queue
    ↓
Dispatcher.Run() wakes up from Dequeue
    ↓
Finds eligible consumer with lowest in-flight
    ↓
Sets DispatchedAt + StatusInFlight on message
    ↓
Increments consumer's in-flight counter
    ↓
Drops message into consumer.Send channel
    ↓
Subscribe goroutine picks it up
    ↓
Sends over the network to the consumer app
    ↓
Consumer processes it and sends ACK back
    ↓
Ack Manager decrements in-flight, writes tombstone to WAL
    ↓
timeout scanner watches DispatchedAt in case ACK never comes
```

The dispatcher is the hinge in the middle. Everything before it is about getting messages in safely. Everything after it is about confirming they were processed.