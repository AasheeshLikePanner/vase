# vase

A pub/sub broker in Go that's 62x faster than Redis at 100 subscribers — built on a lock-free ring buffer where the publisher writes once and every subscriber reads independently.

## The problem

Redis Pub/Sub is single-threaded. Every PUBLISH iterates through all subscribers sequentially in one event loop — O(n) where n is the subscriber count. The more subscribers, the slower it gets.

The numbers show it. Redis throughput collapses from 38,000 messages/sec at 1 subscriber to 1,800 at 100 — a 21x degradation. There's no optimizing around it; it's how the system is designed.

## The fix

Don't iterate through subscribers on each publish. Write once to a lock-free ring buffer. Each subscriber has its own cursor and reads independently, in parallel, at its own pace. The publisher does one atomic operation to advance the head, regardless of how many subscribers exist.

O(1) for the publisher instead of O(n). That's the entire reason for the speedup.

## Benchmarks

Same machine, real TCP connections, Go clients on both sides (go-redis for Redis, native TCP for vase):

| Subscribers | Redis | vase | Speedup |
|-------------|-------|------|---------|
| 1 | 38,000/s | 156,000/s | 4x |
| 10 | 14,700/s | 164,000/s | 11x |
| 50 | 3,500/s | 130,000/s | 37x |
| 100 | 1,800/s | 111,000/s | **62x** |

Redis degrades 21x from 1 to 100 subscribers. vase degrades 1.4x. That gap is the architecture showing up in the numbers.

## Architecture

```
PUBLISH
  → atomically advance ring buffer head
  → store message in slot
  → mark slot ready (sequence number)

SUBSCRIBE (one goroutine per subscriber)
  → read own cursor position
  → check slot sequence matches
  → read message, advance cursor
```

No locks. The atomic head increment is the only coordination point. Subscribers never block each other or the publisher.

## Components

| Component | Description |
|-----------|-------------|
| Ring buffer | Lock-free circular buffer, power-of-2 capacity, bitwise indexing |
| Subscriber cursors | Cache-line padded (64 bytes) to prevent false sharing |
| WAL | mmap'd write-ahead log, 128MB segments, CRC32 per entry, optional |
| Topic manager | Routes publishes, manages subscriptions and cursors |
| Binary protocol | `[op 1B][length 4B][payload]`, big-endian |
| TCP server | One goroutine per connection, 64-message batching |

## Correctness

- 10 concurrent readers + 1 writer, 100k messages — all readers receive all messages
- Fast reader unaffected by slow reader (cursor independence)
- WAL recovery: write 10k messages, kill process, restart, verify all
- Checksum validation: corrupt bytes, verify detection
- **1000 subscribers × 5000 messages = 5,000,000 delivered, zero loss**

## Running

```bash
go run ./cmd/vase/        # start the broker
go run ./bench/           # benchmark vs Redis
go test -race ./...       # full test suite
```

## Limitations

- Publish rate still couples to subscriber count at the TCP layer (the ring buffer is O(1), but the benchmark serializes at the network layer; io_uring fanout would flatten this)
- Single node — no clustering or replication
- Slow subscribers are dropped when their channel fills — no backpressure negotiation

## License

MIT
