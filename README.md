# messager

I wanted to know what actually happens inside a message broker between "publish" and "ack", so I built one. messager is a single Go binary. You send it messages over gRPC, it writes them to disk, and it pushes them to consumers that hold a stream open. It retries a message when a consumer fails it, and it sets aside messages that keep failing.

It's a toy in scale, not in behaviour. You can kill it with `kill -9` halfway through a run, start it again, and it picks up where it left off.

## Try it

You need Go 1.26. Open three terminals:

```bash
# 1: the broker
go run ./cmd/broker

# 2: a quick worker and a sluggish one on the same queue
go run ./cmd/consumer -queue orders -prefetch 10 -id quick &
go run ./cmd/consumer -queue orders -prefetch 2 -delay 200ms -id sluggish

# 3: fifty messages
go run ./cmd/publisher -queue orders -count 50
```

Watch the consumer logs. `quick` ends up with most of the messages. There's no weighting anywhere. The broker just keeps sending each message to whichever consumer has the fewest unacked messages right now.

Next, stop those two consumers and start one that refuses everything:

```bash
go run ./cmd/consumer -queue orders -outcome nack -id grumpy
go run ./cmd/publisher -queue orders -count 1
```

You'll see the same message delivered four times. After that it moves to a queue called `orders.dlq`, which you can read like any other queue:

```bash
go run ./cmd/consumer -queue orders.dlq -id morgue
```

## The life of one message

![System flow](docs/system-flow-2026-04-12-2123.png)

Say a publisher sends `order-7` to `orders`.

The broker gives it an ID (8 random bytes, as hex) and appends it to `data/wal/orders.wal`. It calls `fsync` before replying. Only then does the message go into the in-memory queue, so a publisher never gets an ID for a message that exists only in RAM.

Each queue has one dispatcher goroutine. It starts when the first consumer subscribes to that queue. The dispatcher takes `order-7` off the queue and looks for consumers that are under their prefetch limit. It picks the least loaded one and bumps that consumer's in-flight count. Then it tells the ack manager "this is out with consumer X", and only after that does it hand the message to the stream. The order matters. A fast consumer can ack before the channel send even returns, and if the ack manager hadn't heard about the message yet, it would drop that ack.

If no consumer has room, the dispatcher keeps holding `order-7` and checks again every 5ms. Nothing piles up in memory beyond what's already in the queue. That's all the backpressure there is.

After that, one of three things happens:

- **The consumer acks.** The broker appends a tombstone for that ID to the WAL. The message is now dead to replay.
- **The consumer nacks, goes quiet for longer than `DISPATCH_TIMEOUT`, or disconnects.** The message goes back on the queue with its retry count bumped. On disconnect, only messages still sitting unsent in that consumer's buffer are nacked right away. Anything it already received waits for the timeout.
- **The retry count has already reached `MAX_RETRIES`.** The message is re-published to `orders.dlq`, with a fresh enqueue time.

`orders.dlq` has its own WAL file and its own dispatcher. The one extra rule is that any queue ending in `.dlq` gets swept once an hour, and messages older than `DLQ_TTL` (30 days by default) are tombstoned.

Delivery is at-least-once. A consumer that does the work but dies before its ack reaches the broker will get that message again, so consumers need to cope with duplicates.

## Why it's built this way

**Why push instead of letting consumers poll?** The broker knows every consumer's in-flight count, so it can do load balancing and backpressure in one place. A consumer only has to say how many messages it can handle at once.

**Why one log file per queue?** Replay is simple (read one file, drop everything that has a tombstone), and one busy queue doesn't make another queue's reads slower. The cost is one open file per queue.

**Why is the DLQ just a queue?** It keeps the code small. Dead-lettering is an ordinary `Enqueue` with `.dlq` added to the name, so persistence, replay and consuming all come for free.

**What does a record on disk look like?**

```
C0 FF EE 01 | type | length | { json } | crc32
```

`type` is `01` for a message and `02` for a tombstone. The CRC covers everything after the magic. When the reader finds a record that's cut short or doesn't match its CRC, it treats that as the point where the crash happened. It stops there and keeps everything before it.

For the long versions, see the notes in `docs/`. There's one per component, written while I was building it.

## Breaking it on purpose

`scripts/` holds five end-to-end scenarios. Each one starts a real broker and real consumers:

1. Three consumers at different speeds share a queue. The faster ones should get more messages.
2. A consumer gets killed while it's holding messages. After the timeout, a second consumer should receive them.
3. The broker is killed and restarted. Acked messages should stay gone and unacked ones should come back.
4. A consumer nacks everything. Messages should end up in `orders.dlq`.
5. Fifty messages flood a consumer with prefetch=1 and a 1s delay. Nothing should drop or blow up.

Run them with `make scenarios`, or pick one, for example `make scenario3`. `make test` runs the unit tests with the race detector on.

## Writing a client

The whole API is two RPCs in `proto/broker.proto`: a unary `Publish` and a bidirectional `Subscribe` stream. On `Subscribe`, the first message you send must be `{ queue, prefetch_limit }`. After that, you send `{ message_id, ACK | NACK }` for each message the broker pushes to you.

`node-client/` does exactly this from TypeScript and wraps it in a small HTTP API:

```bash
cd node-client && npm install && npm run dev      # listens on :3001
curl -XPOST localhost:3001/consumer/start -H 'content-type: application/json' -d '{"prefetch":5}'
curl -XPOST localhost:3001/publish -H 'content-type: application/json' -d '{"count":10}'
curl localhost:3001/status
```

After you edit the proto, run `make proto` to regenerate the Go code. You'll need `protoc` and the two Go plugins.

## Knobs

The broker has no config file. Everything is set through environment variables. These are the defaults:

```bash
LISTEN_ADDR=:50051
WAL_DIR=data/wal
MAX_RETRIES=3              # so a message is delivered at most 4 times
DISPATCH_TIMEOUT=30s       # no ack in this long counts as a nack
SCAN_INTERVAL=5s           # how often it checks for timeouts
DLQ_TTL=720h               # set to 0 to keep DLQ messages forever
DLQ_TTL_SCAN_INTERVAL=1h
SHUTDOWN_TIMEOUT=15s
```

On Ctrl-C, the broker stops accepting publishes and lets open streams finish. It then gives in-flight messages up to `SHUTDOWN_TIMEOUT` to get acked. Anything still unacked after that is delivered again on the next start.

The CLI tools print their own flags with `-h`.

## Not done yet

- The WAL never shrinks. The compactor is written and tested, but nothing calls it.
- Every record gets its own `fsync`, and payloads are stored as JSON. It's correct, but it isn't fast.
- After a restart, everything that wasn't acked goes back on the queue, including messages that were in flight when the broker died.
- A dispatcher never stops once it starts, even after every consumer on its queue has left.
- It runs as a single node over plaintext gRPC, with no auth.
