
```go
type Consumer struct {
    ID            string           // unique name for this connection
    Queue         string           // which queue they subscribed to
    Send          chan core.Message // the pipe that delivers messages to them
    PrefetchLimit int32            // max dishes they can handle at once
    ConnectedAt   time.Time        // when they sat down
    inFlight      atomic.Int32     // how many dishes are currently in front of them
}
```

**`Send` channel** — this is the actual delivery pipe. The dispatcher will put messages into this channel. The `Subscribe` goroutine sits on the other end reading from it and pushing messages over the network to the consumer. Nothing goes directly over the network here — the dispatcher just drops a message into the channel and moves on.

**`inFlight`** — this counts messages that have been sent to the consumer but not yet acknowledged. If a consumer said `PrefetchLimit: 3`, it means "never give me more than 3 messages at once." Once 3 are in-flight, the dispatcher stops sending until some are ACKed and the count drops back down.

It's an `atomic.Int32` — not a regular `int` — because two goroutines touch it simultaneously: the dispatcher increments it when sending, the Ack Manager decrements it when an ACK arrives. Using atomic operations means no lock is needed for this field specifically.

**`IsEligible()`** — one line: `inFlight < PrefetchLimit`. The dispatcher calls this before deciding whether to send to this consumer.

---

## `connmgr.go` — The Registry

If `Consumer` is a seat at a table, the `Registry` is the front-of-house — the host who knows every seat in the restaurant, which tables are full, and can instantly answer "who at table Orders is ready for another dish?"

```go
type Registry struct {
    mu      sync.RWMutex
    byQueue map[string]map[string]*Consumer  // "orders" → { id1: C1, id2: C2 }
    byID    map[string]*Consumer             // id1 → C1
}
```

It keeps **two maps** pointing at the same Consumer objects — not two copies. One map is organised by queue, the other by ID. The reason is that two different callers need to look up consumers in two different ways:

- **Dispatcher** asks: *"give me all eligible consumers for the `orders` queue"* → needs `byQueue`
- **Ack Manager** says: *"consumer `a3f9c2d1` just ACKed a message, decrement its in-flight"* → needs `byID`

Without `byID`, the Ack Manager would have to scan every queue looking for one consumer by ID — slow. Without `byQueue`, the dispatcher would have to scan all consumers looking for ones on a particular queue — also slow. Two maps, both O(1) or near-O(1), each optimised for its caller.

**`Register`** — called when a consumer connects. Adds the Consumer pointer to both maps.

**`Deregister`** — called when a consumer disconnects. Looks up the consumer in `byID` (to find which queue they were on), then removes from both maps. If only one map existed you'd need to know the queue ahead of time.

**`EligibleConsumers`** — the dispatcher's main query. Takes a read lock (so multiple dispatcher goroutines for different queues don't block each other), scans the consumers for that queue, and returns only those where `IsEligible()` is true.

**`IncrementInFlight` / `DecrementInFlight`** — both take a read lock to fetch the Consumer pointer from `byID`, then call the atomic operation on the Consumer directly. The lock is released immediately after the pointer is fetched — the actual counter update is lock-free.

---

## How they connect to the rest of the system

```
Consumer connects
    → NewConsumer()           creates the struct, opens the Send channel
    → registry.Register()     puts it in both maps

Dispatcher
    → registry.EligibleConsumers("orders")   finds consumers that can receive
    → registry.IncrementInFlight(id)         marks one more message as sent
    → consumer.Send <- msg                   drops message into the channel

Subscribe goroutine
    → reads from consumer.Send               picks up the message
    → stream.Send(event)                     pushes it over the network to the consumer

Ack Manager
    → registry.DecrementInFlight(id)         consumer confirmed receipt

Consumer disconnects
    → registry.Deregister(id)               cleans up both maps
```

The Consumer's `Send` channel is the handoff point between the Dispatcher (which pulls from the queue and decides who gets what) and the Subscribe goroutine (which owns the network connection). They never talk directly — the channel decouples them completely.