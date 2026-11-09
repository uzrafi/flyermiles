#!/bin/bash
# Creates the FlyerMiles topics. PARTITIONS must be the same for flight.completed and member.profiles:
# the earning engine's stream-table join requires co-partitioned inputs.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
P="${PARTITIONS:-6}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" --partitions "$P" --replication-factor 1 "${@:2}"
}

create flight.completed --config retention.ms=-1  # infinite: reconciliation counts all-time totals
create member.profiles  --config cleanup.policy=compact --config min.compaction.lag.ms=0
create tier.changed
create points.earned    --config retention.ms=-1
create ledger.events    --config retention.ms=-1
$KT --bootstrap-server "$BOOTSTRAP" --list
