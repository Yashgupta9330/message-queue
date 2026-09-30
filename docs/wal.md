## Code ../internal/wal/record.go
## The problem we're solving

We need to save messages to disk so that if the broker crashes, we can recover them. The simplest approach — just write the message text — falls apart immediately:

> How does the reader know where one message ends and the next begins?

If we write three messages back to back, the file looks like:

```
hello worldfoo barsecond message
```

No boundaries. We can't split it back up.

We also have another problem: what if the broker crashes *mid-write*? We get a half-written message at the end of the file. If we try to read that back, we'll corrupt our queue.

The binary record format solves both problems.

---

## The envelope

Think of every message like a physical letter in an envelope. Every envelope looks identical on the outside, regardless of what letter is inside.

```
┌─────────┬──────┬──────────┬───────────┬─────────┐
│  Magic  │ Type │  Length  │  Payload  │  CRC32  │
│ 4 bytes │ 1 B  │  4 bytes │  N bytes  │ 4 bytes │
└─────────┴──────┴──────────┴───────────┴─────────┘
```

Each field has a specific job:

**Magic (4 bytes)** — a fixed number `0xC0FFEE01` written at the start of every record. When the reader sees this, it knows "I'm at the start of a valid record." If it doesn't see this number, something is wrong.

**Type (1 byte)** — what kind of record is this? Either `0x01` (a message being saved) or `0x02` (a tombstone — meaning a message was ACKed and should be ignored on replay). More on tombstones later.

**Length (4 bytes)** — how many bytes is the payload? This is how the reader knows where this message ends and the next one begins. It reads this number first, then reads exactly that many bytes.

**Payload (N bytes)** — the actual message data, JSON encoded. Could be 10 bytes or 10,000 bytes — the length field handles it.

**CRC32 (4 bytes)** — a checksum. After writing type + length + payload, we run a maths function over those bytes that produces a 4-byte fingerprint. On read, we recompute the fingerprint and compare. If they don't match, the write was interrupted mid-way. We stop reading here and don't trust this record.

---

## How the reader walks the file

It doesn't scan character by character. It uses the structure:

```
1. Read 4 bytes → check magic. Wrong? Stop.
2. Read 1 byte  → record type (message or tombstone)
3. Read 4 bytes → payload length (call it N)
4. Read N bytes → the payload
5. Read 4 bytes → the CRC
6. Recompute CRC over [type + length + payload]
7. Match? Great. No match? Torn write — stop.
8. Go to step 1 for the next record.
```

It always knows exactly how many bytes to read because the length field tells it. No guessing.

---

## What a torn write looks like

Normal file with 3 messages:

```
[magic][type][len][payload][crc]  ← message 1, complete
[magic][type][len][payload][crc]  ← message 2, complete
[magic][type][len][pay               ← message 3, crash happened here
```

When the reader gets to message 3, the CRC won't match because the payload is incomplete. The reader stops, throws away the incomplete record, and returns messages 1 and 2. No corruption — the worst case is we lose the last write, which is acceptable for at-least-once delivery.

---

## What a tombstone is

When a consumer ACKs a message, we can't delete it from the middle of the file — it's append-only. So instead we *append a tombstone*: a small record that just says "message ID `abc123` is done, ignore it on replay."

File after message is ACKed:

```
[magic][0x01][len][message abc123...][crc]   ← original message
[magic][0x01][len][message xyz789...][crc]   ← another message
[magic][0x02][len][id: abc123][crc]          ← tombstone for abc123
```

On replay, the reader collects all message records, then applies all tombstones. Message `abc123` gets removed from the result. Only `xyz789` comes back into the queue.

---

## What the code is doing

`encode()` — takes a message, packs it into the envelope described above, returns raw bytes ready to be written to disk.

`decode()` — given raw bytes from disk, checks magic, reads length, reads payload, verifies CRC. Returns the record if everything is valid, or `false` if anything is wrong.

`record` / `recordMsg` — these are just Go structs that describe what goes inside the payload field. They get converted to/from JSON before being placed in the envelope.
