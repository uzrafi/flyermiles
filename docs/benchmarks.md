# Benchmarks

Every number here was measured, by the scripts in [`bench/`](../bench) and the k6 tests in
[`k6/`](../k6). The raw output is in [`bench/results/`](../bench/results).

**Environment.** A single Windows 11 laptop (16 hardware threads, 32 GB RAM). Docker Desktop has a
16 GB limit, and every container shares that one machine: load generator, Kafka, Postgres, Redis, all
services and the monitoring stack. Unless a section says otherwise, the runs use Docker Compose with
**one Kafka broker**. Absolute numbers would be higher on dedicated hardware. The comparisons (before
vs after, 1 vs 6 instances) are like for like.

## Summary

| What | Result |
|---|---|
| End-to-end earn latency at 1,000 flights/s (flight completed → balance updated) | **p50 123 ms, p95 171 ms, p99 194 ms** (119,890 events) |
| End-to-end earn latency at 3,000 flights/s | p50 162 ms, p95 1,475 ms, p99 2,002 ms (269,670 events) |
| Earning-engine throughput, exactly-once, 1 / 3 / 6 partitions and instances | **40,766 / 44,248 / 47,950 events/s** |
| Statement page p95, 100k-entry member: `OFFSET` → keyset + covering index | **47.9 ms → 2.5 ms** (19× faster) |
| Same query at depth 50,000, SQL execution time | 38.1 ms → 0.10 ms |
| Balance reads served from Redis (2,000 reads/s while flights keep arriving) | **95.9% hit rate**: Postgres balance reads cut by 96% |
| Concurrent redemptions, one member, over HTTP | **1,000 at once → 0 overdrafts**; accepted total = exactly what the balance allowed |
| Concurrent redemptions, Testcontainers test (in CI) | 200 at once → 0 overdrafts, 1,512 optimistic-concurrency conflicts retried |
| Read-model rebuild from `ledger.events` | **260,887 entries in 25.4 s** (10,257 entries/s), then 0 reconciliation mismatches |
| Reconciliation after all Compose runs (529,683 flights) | **0 mismatches** |

## 1. End-to-end earn latency

`bench/earn-latency.sh RATE SECONDS`. The simulator stamps each flight with its completion time. The
query-service records `now − completion time` once the points are committed to the read model. That
path covers Kafka → Kafka Streams (exactly-once transaction, 100 ms commit interval) → `points.earned`
→ ledger append (Postgres) → publish queue → relay → `ledger.events` → projection (Postgres). The
numbers come from an HdrHistogram that the script resets before each run.

| Rate | Events | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 1,000/s for 120 s | 119,890 | 123 ms | 171 ms | 194 ms | 348 ms |
| 3,000/s for 90 s | 269,670 | 162 ms | 1,475 ms | 2,002 ms | 2,197 ms |

Both are inside the 5 s SLO. At 3,000/s the ledger's one-transaction-per-earn ingest is the
bottleneck. Batching earns per poll into one transaction is the obvious next step.

## 2. Earning-engine throughput

`bench/engine-throughput.sh "1 3 6" 1000000`. For each P, the script recreates the stack with P
partitions on `flight.completed` and `member.profiles`, preloads 1,000,000 flights with the engine
stopped, then starts P engine instances (1 stream thread each). It times how long the engine's
consumer group takes to drain `flight.completed`. The ledger and query services are stopped, so this
measures the streams app alone, with `exactly_once_v2` on.

| Partitions = instances | Events timed | Seconds | Events/s |
|---|---|---|---|
| 1 | 965,859 | 23.7 | 40,766 |
| 3 | 984,635 | 22.3 | 44,248 |
| 6 | 657,524 | 13.7 | 47,950 |

Adding instances barely helps here, and the reason matters: with one broker on the same machine, the
broker (transactions, fsync, replication bookkeeping) and the shared CPU cap throughput, not the
engine. The test does show that the topology scales out correctly: 6 instances split 6 co-partitioned
tasks with no loss (checked by reconciliation).

**A bug the benchmark caught.** The first scaling run reported the wrong partition counts. Spring's
`KafkaAdmin` saw `NewTopic(partitions=6)` beans in two services and **silently added partitions** to
the existing 1-partition topics. That breaks the co-partitioning the stream–table join depends on. The
fix: services never declare topics. Kafka Streams consumers must also be timed on the stream input
only, because the table-side input keeps a small lag after the stream is drained.

## 3. Statement query: OFFSET vs keyset + covering index

`bench/statement-bench.sh 100000`. One member with 100,000 ledger entries, produced through the real
pipeline. k6 fetches 50-line pages at random depths for 45 s with 10 VUs. For keyset, the setup walks
the statement once to collect a cursor for every page, so both modes fetch the same pages. The index is
`(member_id, occurred_at DESC, seq DESC) INCLUDE (type, points, balance_after)`.

| Query | Index | HTTP p50 | HTTP p95 | HTTP p99 | SQL exec at depth 50,000 |
|---|---|---|---|---|---|
| `OFFSET` | PK only | 41.3 ms | **47.9 ms** | 52.7 ms | 38.1 ms |
| keyset | PK only | 21.4 ms | 30.3 ms | 32.8 ms | 14.4 ms |
| `OFFSET` | covering | 5.9 ms | 11.0 ms | 12.4 ms | 7.6 ms |
| keyset | covering | 1.1 ms | **2.5 ms** | 4.8 ms | **0.10 ms** |

The plans (in full in [`statement-explain.txt`](../bench/results/statement-explain.txt)):

- **Before** (`OFFSET 50000`, no index): `Seq Scan` over all 100,000 rows, then
  `Sort Method: external merge  Disk: 4904kB`; 1,611 shared buffers plus temp files.
- **After** (keyset, covering index): `Index Only Scan using statement_lines_keyset_idx`,
  `Heap Fetches: 0`, **5 buffers**, 50 rows read.

`OFFSET` with the index is still O(depth): it walks 50,050 index entries to return 50. Keyset seeks
straight to the cursor, so its cost doesn't depend on how deep the page is.

## 4. Redis cache-aside for balances

`bench/cache-bench.sh 2000 100`. 2,000 balance reads/s for 60 s over a 2,000-member working set, while
100 flights/s keep arriving (so cache invalidation is real). Run once with the cache disabled at
runtime, then once with it enabled.

| | p50 | p95 | p99 | Postgres balance queries |
|---|---|---|---|---|
| Cache off | 0.52 ms | 0.56 ms | 2.09 ms | every request |
| Cache on | 0.52 ms | 1.03 ms | 2.07 ms | 4.1% of requests (hit rate **95.9%**: 114,945 hits, 4,978 misses) |

**Honest reading:** on one machine, a primary-key lookup in a warm Postgres is as fast as a Redis round
trip, so the cache did not lower latency (p95 was slightly higher). What it did was take **96% of
balance reads off Postgres**. That is the point of the cache: protecting the database from read
traffic. It's what a resume claim should say, not "faster reads".

## 5. Concurrent redemptions (no overdrafts)

**Over HTTP**, `bench/redemption-bench.sh 1000`: a member is funded through the real earn path to
63,217 points. Then 1,000 k6 VUs each send one 100-point redemption at the same instant, through the
redemption-service (read version → write with `expectedSeq` → retry on 409 with full jitter) and the
ledger-service.

| Concurrent | Accepted | Rejected (insufficient) | Overdrafts | Points redeemed | Max possible | Final balance |
|---|---|---|---|---|---|---|
| 1,000 | 632 | 367 | **0** | 63,200 | 63,200 | 17 |

One client used up its 50-retry budget without writing anything (632 + 367 = 999). p95 was 9.9 s:
1,000 writers on one account serialize on that account's version, by design.

The first attempt failed with "connection refused" on 758 requests. Tomcat's default accept backlog
(100) overflowed, and Windows refuses instead of queueing. The fix was `server.tomcat.accept-count`,
plus treating a refused connection as retryable. That's safe because every retry carries the same
`Idempotency-Key`.

**In CI** (`ConcurrentRedemptionTest`, Testcontainers Postgres): 200 redemptions race for a balance
that covers 100. Result: 100 accepted, 100 rejected, 1,512 conflicts retried, 0 overdrafts, final
balance 0.

## 6. Read-model rebuild

`bench/rebuild-bench.sh`: truncate `member_balances` and `statement_lines`, rewind the query-service
consumer group to the start of `ledger.events`, and time the replay up to the end offsets captured at
the start. The run used 2 listener threads.

| Entries | Time | Rate | Reconciliation afterwards |
|---|---|---|---|
| 260,887 | 25.4 s | 10,257/s | 0 mismatches |

## 7. Kubernetes

See [kubernetes.md](kubernetes.md) for the kind cluster results: Strimzi with 3 brokers, failure tests
and KEDA scaling.
