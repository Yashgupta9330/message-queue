# Stress Dashboard Plan

## Goal

A browser-based control panel that lets you spawn consumers, fire concurrent publishers, and watch live stats — all without touching the terminal after startup.

---

## What already exists

| Endpoint | What it does |
|---|---|
| `POST /consumer/start` | Spawns one consumer `{queue, prefetch, id}` |
| `POST /consumer/stop` | Stops one or all consumers `{id?}` |
| `POST /publish` | Publishes up to 1000 msgs sequentially |
| `GET /status` | Live snapshot of publisher stats + all consumers |

---

## What needs to be built

### Node client changes (`node-client/src/index.ts`)

**1. Remove the 1000-message hard cap** (`index.ts:295`)
The cap was a safety guard; the frontend becomes the control plane instead.

**2. `POST /load-test/start`**
```json
{ "consumers": 10, "publishers": 5, "events_per_publisher": 200, "queue": "orders" }
```
- Spawns N consumers via `startConsumer()` × N
- Fires M concurrent publisher loops via `Promise.all` — each calls `publishOne()` K times
- Sets a `loadTestActive` flag readable by `/status`

**3. `POST /load-test/stop`**
- Cancels all consumers
- Sets `loadTestActive = false`

**4. `express.static('public')`**
Serve the frontend HTML directly from Express — no separate dev server needed.

---

### Frontend (`node-client/public/index.html`)

Single HTML file, vanilla JS, no build step. Polls `GET /status` every 500ms.

```
┌─────────────────────────────────────────────────────┐
│  MESSAGER STRESS DASHBOARD                          │
├────────────────┬────────────────┬───────────────────┤
│ Consumers [10] │ Publishers [5] │ Events/pub  [200] │
├────────────────┴────────────────┴───────────────────┤
│      [ ▶  START LOAD TEST ]    [ ■  STOP ALL ]      │
├─────────────────────────────────────────────────────┤
│  Published: 1,000    Errors: 0    Active consumers: 10 │
├─────────────────────────────────────────────────────┤
│  Consumer ID    Queue    Received    Acked           │
│  consumer-1     orders   47          47              │
│  consumer-2     orders   53          53              │
│  ...                                                │
└─────────────────────────────────────────────────────┘
```

---

## Data flow

```
Browser ──POST /load-test/start──▶ Node Client
                                        │
                                startConsumer() × N
                                Promise.all(publishOne() × M × K)
                                        │
                                gRPC streams ──▶ Go Broker ──▶ WAL
                                        │
Browser ◀──GET /status (500ms poll)─── Node Client
```

---

## Known bottlenecks to watch

| Layer | Bottleneck | Signal |
|---|---|---|
| WAL | `fsync` on every publish — ~2k–5k msg/s ceiling on SSD | Publish latency climbs |
| Registry | `EligibleConsumers` is O(n) under `RLock` | CPU spikes past ~200 consumers |
| Consumer channel | `Send chan` buffered to `prefetchLimit`; slow consumer blocks dispatcher | Messages queue up in topic |
| gRPC / OS | No `MaxConcurrentStreams` set; macOS default fd limit is 256 | Connection errors past ~250 consumers |

Run `ulimit -n 4096` before starting the broker when testing at high consumer counts.

---

## Files to create / modify

```
node-client/
  src/index.ts          ← add /load-test/start, /load-test/stop, raise cap, add static serving
  public/index.html     ← new: the dashboard
```

No changes to the Go broker required.
