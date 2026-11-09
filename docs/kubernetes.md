# Kubernetes results

All measured on a local **kind** cluster (1 control plane + 2 workers, Kubernetes 1.34) on the same
laptop as [benchmarks.md](benchmarks.md). `scripts/k8s-up.sh` builds the whole thing:

- **Kafka 4.1 run by the Strimzi 0.48 operator**: 3 KRaft nodes (controller + broker), replication
  factor 3, `min.insync.replicas=2`. `KafkaTopic` resources for the 5 topics, and one `KafkaUser` per
  service with SCRAM-SHA-512 authentication and least-privilege ACLs. The earning engine, for example,
  may read its inputs, write its outputs, and manage only `earning-engine-*` internal topics and
  transactional IDs.
- **One Helm chart per service**, all built on a shared library chart. Each chart provides Actuator
  liveness/readiness probes (readiness includes the DB, or the Kafka Streams state), resource requests
  and limits, a `PodDisruptionBudget`, a `ConfigMap` and `Secret`, a `ServiceMonitor`,
  `maxUnavailable: 0` rolling updates, a `preStop` delay, and topology spread across nodes.
- **KEDA** scales the query-service on consumer lag of `ledger.events` (1 to 6 replicas, never more
  than the partition count). A `NetworkPolicy` lets only the redemption-service, plus Prometheus, reach
  the ledger API. Reconciliation runs as a `CronJob`.
- **Monitoring**: kube-prometheus-stack (Prometheus, Alertmanager, Grafana), Loki, Tempo, the
  OpenTelemetry Collector and the Pushgateway. The `PrometheusRule` holds the SLOs and alerts.

## Failure tests: 0 events lost or duplicated

`bench/k8s-failure-tests.sh`. Each test runs under load from the simulator (500 flights/s) and is
followed by the reconciliation job. Reconciliation counts every record in `flight.completed` and
`points.earned` (read_committed), every ledger entry, and every read-model row and balance.

| Test | What happened | Flights reconciled | Mismatches |
|---|---|---|---|
| Delete an earning-engine pod mid-load | its tasks moved to the other instance; the replacement pod restored state from the changelogs | 1,415,055 | **0** |
| Rolling restart of a Kafka broker (`strimzi.io/manual-rolling-update`) | Strimzi replaced `flyermiles-dual-role-1` while producers and Streams transactions kept running | 1,461,570 | **0** |
| Kill the query-service during a read-model rebuild (1.46M entries) | the new pod resumed from the committed offsets; replayed batches were absorbed by the idempotent projection | 1,461,570 | **0** |
| Rolling update of the earning-engine during load | `maxUnavailable: 0`; the old instances' transactional producers were fenced, then their work moved over | 1,493,245 | **0** |

Every check passed: the 7 checks listed in [`Reconciler.java`](../reconciliation-job/src/main/java/dev/flyermiles/recon/Reconciler.java).
Raw output: `bench/results/k8s-recon-*.txt`.

## KEDA: lag-driven scale-out

`bench/k8s-keda.sh 1000000`. Under steady load the ledger is the slowest stage, so a single
query-service replica never falls behind, and KEDA (correctly) doesn't scale. The test models backlog
recovery instead. The query-service is paused at 0 replicas, as in an outage, while 1,000,000 flights
flow through the ledger. Then the pause is lifted. Lag comes from Strimzi's Kafka Exporter through
Prometheus, so it updates every 5–10 s.

| t (s) | Lag on `ledger.events` | Ready replicas |
|---|---|---|
| 0 | 1,000,000 | 0 |
| 10 | 1,000,000 | 1 |
| 16 | 918,000 | 1 |
| 26 | 741,000 | 3 |
| 37 | 332,000 | 3 |
| 42 | 332,000 | 5 |
| 47 | 0 | 5 |

**KEDA scaled the query-service from 1 to 5 replicas and the 1,000,000-event backlog was gone in about
47 s** (± one Prometheus scrape). It stopped at 5 replicas because the lag was already near zero when
the next scale step would have happened. With a 200k backlog, one replica drained it in about 20 s,
before KEDA's scale-out mattered: the batched projection does about 10k events/s per replica.

## Tracing

The OpenTelemetry Java agent carries trace context through Kafka headers. A single trace in Tempo shows:

```
flight-activity-simulator  flight.completed publish            (root)
earning-engine             flight.completed process
earning-engine             points.earned publish
ledger-service             points.earned process
ledger-service             SELECT ledger_entries / INSERT ledger_entries
```

The relay that publishes `ledger.events` starts a new trace, because it reads entries back from
Postgres. Storing the `traceparent` with each ledger entry would join the two halves. Logs reach Loki
over OTLP with `trace_id` as structured metadata, so Grafana links a span to its log lines.

Traces are sampled at 10% (parent-based, so a kept trace is complete). At 100%, the million-event runs
pushed Tempo past its ingestion rate limit and memory limit and got it OOM-killed.

## Problems hit along the way (and fixes)

- **cgroup v1.** Docker Desktop on an older WSL2 kernel (5.15) provides cgroup v1. From Kubernetes 1.35
  the kubelet refuses to start on it, so the kind nodes are pinned to 1.34 (`wsl --update` removes the
  need).
- **Strimzi 0.44** couldn't parse the Kubernetes 1.34 version response (a new `emulationMajor` field)
  and crash-looped. Moving to 0.48 with Kafka 4.1 fixed it. The services' Kafka 3.7 clients work with
  4.x brokers unchanged.
- **Secret name clash.** The Strimzi User Operator names each KafkaUser's secret after the user, which
  collided with each chart's DB secret, so those are now `<service>-db`.
- **Cross-namespace DNS.** KEDA's operator lives in the `keda` namespace, so the Kafka bootstrap
  address has to be fully qualified (`flyermiles-kafka-bootstrap.flyermiles.svc:9092`).
