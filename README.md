# FlyerMiles: an event-sourced loyalty points ledger

An airline loyalty program (earn points on flights, redeem them, track elite tier status) built as
**Java 21 / Spring Boot microservices** that talk only through **Kafka**. Points are calculated by
**Kafka Streams** with exactly-once processing, stored in an append-only **event-sourced ledger**, and
served through a separate **CQRS read side** with tuned PostgreSQL queries and a Redis cache. It all
runs on **Kubernetes** with Kafka managed by the **Strimzi** operator, autoscaled on consumer lag by
**KEDA**, and observed end to end with **OpenTelemetry, Prometheus, Grafana, Loki and Tempo**.

FlyerMiles is a fictional program; no real airline's data or branding is used.

## Results (measured)

All on a single laptop. Method, raw output and caveats are in [docs/benchmarks.md](docs/benchmarks.md)
and [docs/kubernetes.md](docs/kubernetes.md).

| | |
|---|---|
| End-to-end earn latency at 1,000 flights/s (flight → balance updated) | p50 **123 ms**, p95 **171 ms**, p99 194 ms |
| Kafka Streams throughput, exactly-once, 6 partitions/instances | **~48,000 events/s** (single broker) |
| Statement page p95 on a 100k-entry account: `OFFSET` → keyset + covering index | **47.9 ms → 2.5 ms** |
| Balance reads served from Redis | **95.9%** hit rate (96% fewer Postgres reads) |
| 1,000 concurrent redemptions on one account | **0 overdrafts**; accepted exactly what the balance allowed |
| Pod kill, broker rolling restart, kill during rebuild, rolling update (Kubernetes, under load) | **0 events lost or duplicated** across 1.49M flights |
| KEDA scale-out on a 1M-event backlog | 1 → **5 replicas**, backlog drained in **~47 s** |
| Read-model rebuild from `ledger.events` | 260,887 entries in **25.4 s** |

## Architecture

```
  flight-activity-simulator ──▶ flight.completed (Avro, key = memberId, 6 partitions)
                                         │
  member-service ──▶ member.profiles (compacted, 6 partitions) ──▶ KTable(memberId → tier)
     ▲                                   │
     │ tier.changed                      ▼
     │                ┌───────────────────────────────────────────┐
     └────────────────│ earning-engine (Kafka Streams)            │
                      │  flights ⋈ tier KTable → points           │
                      │  calendar-year window → qualifying miles  │
                      │  processing.guarantee = exactly_once_v2   │
                      └─────────────────────┬─────────────────────┘
                                            ▼ points.earned (read_committed)
  redemption-service ─── POST (expectedSeq) ─▶ ┌──────────────────────────────────┐
   Idempotency-Key, retries with full jitter   │ ledger-service (event store)     │
                                               │  ledger_entries: append-only     │
                                               │  PK (member_id, seq) = optimistic│
                                               │  concurrency; entry_id = dedupe  │
                                               │  publish queue → relay           │
                                               └───────────────┬──────────────────┘
                                                               ▼ ledger.events
                                               ┌──────────────────────────────────┐
                                               │ query-service (CQRS read side)   │
                                               │  member_balances, statement_lines│
                                               │  keyset pagination + covering idx│
                                               │  Redis cache-aside, ?minSeq=     │
                                               └──────────────────────────────────┘
  reconciliation-job (CronJob): Kafka counts vs ledger vs read model, pushed to Prometheus
```

| Service | What it does | Techniques |
|---|---|---|
| [flight-activity-simulator](flight-activity-simulator) | Realistic completed flights at a set rate, or in bursts | Avro producer, event-time timestamps |
| [member-service](member-service) | Enrolment, profiles, applies tier upgrades | Compacted topic used as a table |
| [earning-engine](earning-engine) | Points and qualifying miles | Stream–table join, custom calendar-year window, RocksDB, `exactly_once_v2` |
| [ledger-service](ledger-service) | Source of truth: EARNED / REDEEMED / REVERSED entries | Event sourcing, optimistic concurrency on `(member_id, seq)`, snapshots, append-only enforced by triggers |
| [redemption-service](redemption-service) | `POST /redemptions` | `Idempotency-Key`, expected-version writes, full-jitter retries |
| [query-service](query-service) | Balances and statements | Idempotent projections, keyset pagination, covering index, Redis cache-aside, read-your-writes |
| [reconciliation-job](reconciliation-job) | Proves the totals add up | Kubernetes CronJob, Prometheus Pushgateway, critical alert |

## Design notes

**The ledger is the only source of truth.** `ledger_entries` has no balance column to update: the
balance is the sum of a member's entries, and triggers reject `UPDATE`, `DELETE` and `TRUNCATE`. A
cancelled flight is undone by appending a compensating `REVERSED` entry. Every 100 entries a snapshot
row bounds how much history is read to get the current state.

**Optimistic concurrency, not locks.** A writer reads the member's latest `seq`, decides (is the
balance enough?), and inserts `seq + 1`. If two writers read the same version, the primary key lets
exactly one in; the loser gets `409`, re-reads and decides again. A `CHECK (balance_after >= 0)` on
redemptions is the database-level backstop.

**No dual writes.** An `AFTER INSERT` trigger records each new entry in a publish queue in the same
transaction. A relay (one active replica, held by a transaction-scoped advisory lock) publishes batches
to `ledger.events` and deletes them from the queue only after Kafka acknowledges them. Delivery is
at-least-once; the read side is idempotent on `(member_id, seq)`.

**Exactly-once where Kafka can give it.** The earning engine reads, updates its RocksDB state stores
and writes `points.earned` / `tier.changed` in one Kafka transaction. Downstream consumers use
`isolation.level=read_committed`. The ledger dedupes on the flight's `eventId`, which carries the
guarantee across into Postgres.

**Co-partitioning.** `flight.completed` and `member.profiles` are both keyed by `memberId` with 6
partitions, so a member's flights and profile are on the same stream task and the join needs no
repartition topic.

**Calendar-year windows.** Kafka Streams' `TimeWindows` are fixed-size from the epoch, so a
"365-day" window drifts off the calendar. [`CalendarYearWindows`](earning-engine/src/main/java/dev/flyermiles/earning/CalendarYearWindows.java)
aligns windows to January 1 in a configured time zone, including leap years.

**Read-your-writes.** A redemption returns its ledger `seq`. `GET /members/{id}/balance?minSeq=…`
waits up to 1 s for the projection to reach that version, then answers `202` with the current value.

**Schema evolution.** Avro schemas live in [schemas/](schemas) and are registered in Apicurio with
`BACKWARD` compatibility. `FlightCompleted` v2 adds an optional `cabin`;
[`FlightCompletedEvolutionTest`](schemas/src/test/java/dev/flyermiles/avro/FlightCompletedEvolutionTest.java)
shows v1 readers handle v2 data and a breaking change is rejected.

**Monitoring.** Each service runs the OpenTelemetry Java agent, which traces HTTP, JDBC and Kafka and
carries the trace context in Kafka headers. The agent sends traces to Tempo, logs to Loki and its own
metrics to Prometheus through the OpenTelemetry Collector. Prometheus also scrapes application metrics
from each service's Actuator endpoint. In Grafana you can go from a slow request to its trace in
Tempo, and from a span to its log lines in Loki.

## Running it

Prerequisites: JDK 21, Docker Desktop (16 GB of memory), and for Kubernetes: kind, kubectl, Helm,
and k6 for the load tests.

### Docker Compose (development, benchmarks)

```bash
./mvnw -DskipTests package
docker compose --profile apps --profile obs up -d --build
curl -X POST localhost:8081/members/bulk -H 'Content-Type: application/json' -d '{"count":10000}'
curl -X POST 'localhost:8085/simulator/start?rate=200'
curl localhost:8084/members/<memberId>/balance
docker compose run --rm reconciliation-job
```

Grafana is at http://localhost:3000.

### Kubernetes (kind, 3 nodes)

```bash
scripts/k8s-up.sh       # kind (k8s 1.34), Strimzi 0.48 + Kafka 4.1, KEDA, monitoring, images, all charts
scripts/port-forward.sh # Grafana on :3000, APIs on :8081-8085
```

### Tests

```bash
./mvnw verify
```

This runs the `TopologyTestDriver` tests for the streams logic, the Testcontainers concurrency test (N
simultaneous redemptions on one member, which must never overdraw), the projection idempotency and
pagination tests, and the schema-compatibility test.

## Repository layout

```
schemas/                 Avro schemas (+ generated classes) shared by all services
<service>/               one Maven module per service
deploy/compose/          Docker Compose helpers (topic creation, DB init)
deploy/observability/    OTel Collector, Prometheus, alerts, Loki, Tempo, Grafana (Compose)
deploy/helm/             flyermiles-lib (library chart) + one chart per service + infra
deploy/k8s/              kind config, Strimzi Kafka/topics/users, monitoring values
k6/                      load tests
bench/                   benchmark scripts and raw results
docs/                    benchmarks and failure-test write-ups
```
