# Pulse — Building a Pub/Sub Broker That's 62x Faster Than Redis

## The Problem: Why Redis Pub/Sub Breaks at Scale

I was working on a system that needed to push updates to thousands of subscribers in real-time. Redis Pub/Sub seemed like the obvious choice — it's battle-tested, well-documented, and everyone uses it. But when I benchmarked it with 100 subscribers, something was wrong. The throughput had collapsed from 38,000 messages per second at 1 subscriber to just 1,800 at 100. That's a 21x degradation.

Why? Redis Pub/Sub is single-threaded. Every PUBLISH command runs in one event loop that iterates through all subscribers sequentially. At 100 subscribers, Redis performs 100 socket writes in a row before returning control to the publisher. The math is simple: O(n) where n = subscriber count. The more subscribers you have, the slower it gets.

This is a fundamental architectural limitation. There's no way to optimize around it within Redis — it's how the system was designed.

## The Insight: Write Once, Read N Times in Parallel

What if the publisher wrote to a data structure once, and each subscriber read from it independently at their own pace? The publisher doesn't wait for anyone. Subscribers don't block each other. That's the core insight behind Pulse.

Instead of iterating through subscribers on each publish, we write to a ring buffer. Each subscriber has their own cursor — a position in the buffer. They read messages in parallel, completely independently. The publisher does one atomic operation to advance the head pointer, and that's it. N subscribers can read from the same message without any coordination between them.

This is O(1) for the publisher regardless of subscriber count. The ring buffer is the key.

## How It Works

### The Ring Buffer

The ring buffer is a lock-free circular buffer. It has a head (write position) and slots for messages. When a publisher writes:

1. Atomically increment the head
2. Store the message in the slot (head - 1)
3. Mark the slot as ready by storing the sequence number

When a subscriber reads:

1. Load their cursor position
2. Check if the slot's sequence matches what they expect
3. If yes, read the message and advance cursor

The magic is that no lock is needed. The atomic head increment is the only coordination point. Each subscriber reads independently — they don't block each other, they don't block the writer.

We use power-of-2 capacity and bitwise AND for indexing (faster than modulo). The sequence number check ensures readers don't read uninitialized slots.

### Write-Ahead Log for Durability

A ring buffer in memory loses everything on crash. We need durability. That's where the WAL comes in.

The WAL (Write-Ahead Log) stores every message to disk before it's considered published. We use memory-mapped files (mmap) for zero-copy writes — the kernel handles page faults transparently, and we avoid the syscall overhead of traditional file writes.

Each segment is 128MB. When one fills up, we create a new one. Every entry has a CRC32 checksum so we can detect corruption on recovery.

The WAL architecture:
- Write: append entry, compute checksum, store
- Recovery: scan segments, validate checksums, resume from last valid offset

This is optional — you can disable durability for maximum speed if you don't need it.

### Topic Manager

The topic manager sits between the server and the ring buffer. It:

- Creates topics on demand
- Routes publishes to the right ring buffer
- Manages subscriptions and their cursors
- Starts a goroutine for each subscriber that reads from the ring buffer and pushes to their channel

Each subscription has cache-line padding (64 bytes) around the cursor to prevent false sharing. If two cursors share a cache line, writing to one invalidates the other on a different CPU core — that kills performance.

### Binary Protocol

We built a simple binary protocol instead of using JSON or Protobuf:

```
[op 1 byte][frame length 4 bytes][payload]
```

Operations: PUBLISH, SUBSCRIBE, ACK, DELIVER, OK, ERR

All integers are big-endian. Maximum topic length: 255 bytes. Maximum message: 64MB.

### TCP Server

The server accepts connections and routes messages. One goroutine per connection — idiomatic Go. We use bufio for read/write buffering (64KB each).

We also implemented message batching: accumulate up to 64 messages before flushing to TCP. This reduces syscall overhead by 64x.

## The Numbers

We benchmarked both systems on the same machine with real TCP connections. Both use Go clients for fair comparison (go-redis for Redis, native TCP for Pulse).

| Subscribers | Redis | Pulse | Speedup |
|-------------|-------|-------|---------|
| 1 | 38,000/s | 156,000/s | 4x |
| 10 | 14,700/s | 164,000/s | 11x |
| 50 | 3,500/s | 130,000/s | 37x |
| 100 | 1,800/s | 111,000/s | **62x** |

The internal ring buffer (no network overhead):
- 100 subscribers: 1.3 billion messages/second fanout
- 1000 subscribers: 3.5 billion messages/second fanout

Redis degrades 21x from 1 to 100 subscribers. Pulse degrades 1.4x. That's the architectural difference showing up in the numbers.

## Correctness First

Benchmarks are easy to fudge. What's harder is building confidence that the system actually works correctly. We wrote tests at every level:

**Ring Buffer Tests:**
- RB-1: Sequential correctness — write N, read all in order
- RB-2: 10 concurrent readers + 1 writer, 100k messages — all readers get all messages
- RB-3: Fast reader isn't slowed by slow reader (cursor independence)
- RB-4: Buffer overflow behavior

**WAL Tests:**
- WAL-1: Recovery after crash — write 10k messages, kill process, restart, verify all
- WAL-2: Segment rotation — fills 128MB segment, creates second
- WAL-3: Concurrent writers — 8 goroutines writing 10k each = 80k total
- WAL-4: Checksum validation — corrupt bytes, verify detection

**Protocol Tests:**
- P-1: Round-trip encode/decode — 100k random valid frames
- P-2: Malformed frame handling
- P-3: Max size enforcement

**Topic/Server Tests:**
- Single producer, single subscriber, ordered delivery (100k messages)
- Connection lifecycle
- End-to-end publish/subscribe

The biggest test: **1000 subscribers × 5000 messages = 5,000,000 delivered, zero loss**. That's what separates a real system from a toy benchmark.

## Honest Limitations

This isn't a "we solved everything" post. Here are the real limitations:

**Publish rate still couples to subscriber count at the network layer.** The benchmark measures end-to-end: PUBLISH → wait for N subscribers → ACK. The ring buffer itself is O(1) regardless of subscribers, but we're measuring at the TCP layer where we still serialize. Async publish with io_uring fanout would make publish rate completely flat.

**Single node only.** No clustering, no replication. This is a proof-of-concept architecture, not a production system.

**Slow subscriber drops.** If a subscriber's channel fills up, we close their connection. No backpressure negotiation — fire and forget.

## Why This Matters

The fundamental insight is simple: don't iterate through subscribers on each publish. Write once to a shared data structure, let each subscriber read independently. That's the difference between O(n) and O(1).

Redis is an incredible system — I use it daily. But its pub/sub was designed for a different era with fewer subscribers. For systems that need to scale to hundreds or thousands of subscribers, the ring buffer architecture is fundamentally better.

The code is on GitHub. Try it yourself. Run the benchmarks. The numbers don't lie.

---

*Thanks for reading. If you have questions about the architecture or want to contribute, open an issue.*# vase
