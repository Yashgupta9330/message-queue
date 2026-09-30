
Our proto file defined two calls:

```
Publish  — producer sends one message, broker replies with its ID
Subscribe — consumer opens a long-lived two-way channel to receive messages and send ACKs
```

---

## Part 1 — Publish

This is the simple one. A producer calls `Publish` like a regular function call:

1. Producer sends: `{ queue: "orders", payload: "buy 5 apples" }`
2. Broker generates a unique random ID for the message
3. Broker writes it to the WAL (Phase 1), then puts it in the in-memory queue (Phase 2)
4. Broker replies: `{ message_id: "a3f9c2d1..." }`

That's it. One request, one response, connection closes. The code for this is `Publish()` in `server.go` — it's 20 lines and straightforward.

---

## Part 2 — Subscribe (the complex one)

Subscribe is fundamentally different. Instead of one request → one response, it's a **permanently open two-way channel** — like a phone call that stays open.

**Why two-way?** Because the consumer needs to do two things simultaneously:
- **Receive** messages being pushed to it from the broker
- **Send** ACKs back to the broker saying "I processed that one"

Both of these happen at the same time, on the same connection.

### How the connection opens

The consumer opens the stream and **immediately sends the first message** saying which queue it wants:

```
Consumer → Broker: { subscribe: { queue: "orders", prefetch_limit: 1 } }
```

The broker reads this first message (`stream.Recv()`) and uses the queue name to know where to pull messages from. If the consumer sends anything other than this as the first message, the broker rejects it.

### The two goroutines

Once the queue name is known, the broker spawns two goroutines that run simultaneously:

**Send goroutine** (broker → consumer):
```
loop forever:
    block on Dequeue("orders")  ← waits until a message arrives
    check if consumer disconnected → if yes, stop
    send the message over the stream to the consumer
```

**Recv goroutine** (consumer → broker):
```
loop forever:
    wait for the consumer to send something
    if it's an ACK → hand it to the Ack Manager (Phase 6, stubbed for now)
    if it's an error/disconnect → stop
```

Both goroutines signal on a shared `done` channel when they finish. The `Subscribe` function just waits:
```go
return <-done  // blocks until one goroutine stops
```

As soon as one goroutine signals (e.g. consumer disconnected), `Subscribe` returns and the stream closes.

### One subtle thing about the send goroutine

`Dequeue` blocks forever waiting for a message. If the consumer disconnects while the queue is empty, the goroutine is stuck in `Dequeue` — it has no way to know the consumer is gone. It will stay there until either:
- A new message arrives (then it checks `ctx.Done()` and exits cleanly)
- The broker shuts down (which closes the topic and unblocks Dequeue)

This is intentional and acceptable — at most one leaked goroutine per consumer connection.


## The full picture

```
Producer app
    │
    │  Publish("orders", payload)  [gRPC]
    ▼
┌─────────────────────────────────┐
│  server.go – Publish()          │
│  1. generate ID                 │
│  2. call manager.Enqueue()      │
│     → write WAL (Phase 1)       │
│     → add to topic (Phase 2)    │
│  3. return ID                   │
└─────────────────────────────────┘
                │
                │ message sits in topic
                │
┌─────────────────────────────────┐
│  server.go – Subscribe()        │
│  send goroutine:                │
│    Dequeue() ← blocks           │
│    got message → stream.Send()  │◄── pushes to consumer
│  recv goroutine:                │
│    stream.Recv() ← blocks       │◄── reads ACKs from consumer
└─────────────────────────────────┘
    │
    │  stream of messages  [gRPC]
    ▼
Consumer app
    │
    │  AckRequest back  [gRPC]
    ▼
