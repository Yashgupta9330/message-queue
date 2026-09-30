## `topic.go` — the actual queue

This is just a list of messages with two operations: put something in, take something out. First in, first out.

```go
type Topic struct {
    mu     sync.Mutex
    cond   *sync.Cond
    msgs   []core.Message
    closed bool
}
```

`msgs` is the list. `mu` is a lock so two goroutines don't touch the list at the same time. `cond` is the interesting part — explained below.

**Enqueue** — adds a message to the back of the list, then calls `Signal()`.

**Dequeue** — this is where `cond` matters. The dispatcher goroutine calls `Dequeue` and just sits there waiting for work. But if the queue is empty, what should it do? Busy-loop and keep checking? That wastes CPU. Sleep for a fixed time? That adds latency.

`sync.Cond` solves this cleanly. `Wait()` puts the goroutine to sleep and releases the lock. When `Enqueue` calls `Signal()`, it wakes the sleeping goroutine back up. So Dequeue costs zero CPU while the queue is empty, and wakes up the instant something arrives.

```go
for len(t.msgs) == 0 && !t.closed {
    t.cond.Wait()  // sleep here, release lock, wake on Signal
}
```

It's a `for` loop not an `if` because of spurious wakeups — `Wait` can return even when nothing called `Signal`, so you re-check the condition.

**Close** — when the broker shuts down, the dispatcher goroutine is asleep inside `Dequeue`. `Close` sets `closed = true` and calls `Broadcast` (wakes all sleeping goroutines). The goroutine wakes up, sees the queue is empty and closed, returns `false`. Dispatcher knows to stop.

---

## `queue.go` — the manager

Topic is just the data structure. Manager is the layer that connects it to the WAL (disk) and handles multiple queues at once.

```go
type Manager struct {
    walDir  string
    topics  map[string]*Topic   // one Topic per queue name
    writers map[string]*wal.Writer  // one WAL file per queue name
}
```

**Why does Manager exist?** Topic lives in memory. If the broker crashes, it's gone. Manager's job is to make sure every message is written to disk *before* it goes into the Topic. On restart, Manager reads the disk back and refills the Topic.

**NewManager** — on startup, scans the WAL directory for `.wal` files. For each one it finds, it calls `initQueue`.

**initQueue** — this does three things in order:
1. Reads the WAL file and gets back the messages that survived (not tombstoned)
2. Opens the WAL file for writing (so new messages can be appended)
3. Creates a Topic and pre-loads it with the replayed messages

This is how the broker recovers after a crash — the queue is restored from disk.

**Enqueue** — this is the critical invariant of the whole system:

```
WAL write → in-memory enqueue
```

Never the other way around. If the WAL write fails, the message is dropped and the error goes back to the caller. If the broker crashes after the WAL write but before the in-memory enqueue, the next `NewManager` call replays the WAL and the message reappears. You can never lose a message that was successfully written to the WAL.

Notice the lock is released *before* the WAL write:

```go
m.mu.Lock()
// just grab the writer and topic pointers
w := m.writers[msg.Queue]
t := m.topics[msg.Queue]
m.mu.Unlock()

w.WriteMessage(msg)  // slow disk write, done outside the lock
t.Enqueue(msg)
```

This matters because `WriteMessage` calls `fsync` which can take milliseconds. Holding the lock that long would block every other goroutine trying to Dequeue.

**Dequeue** — grabs the topic pointer under the lock, releases the lock, then calls `t.Dequeue()`. The blocking happens outside the lock so Enqueue can still run concurrently.

**WriteTombstone** — writes an ACK record to the WAL saying "this message is done, don't replay it". Called by the Ack Manager in Phase 6 when a consumer acknowledges a message.

**Close** — shuts everything down in the right order: first closes all Topics (which unblocks any sleeping Dequeue calls), then closes all WAL writers (flushing anything buffered).

---

## How they fit together

```
Producer → Manager.Enqueue()
               ├── write to WAL file (disk, durable)
               └── Topic.Enqueue() (memory)

Dispatcher → Manager.Dequeue()
               └── Topic.Dequeue() (blocks if empty, wakes on Signal)

Consumer ACKs → Manager.WriteTombstone() (disk)

Crash + restart → NewManager() replays WAL → Topic refilled
```

The WAL is the source of truth. The Topic is just a fast in-memory view of what's in the WAL.