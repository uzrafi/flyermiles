// Generates dashboards/flyermiles.json. Run: node deploy/observability/grafana/dashboard.gen.js
const fs = require("fs");
const path = require("path");

const ds = { type: "prometheus", uid: "prometheus" };
let id = 1;
let y = 0;
const panels = [];

function row(title) {
  panels.push({ id: id++, type: "row", title, collapsed: false, gridPos: { h: 1, w: 24, x: 0, y: y++ } });
}

function ts(title, targets, { w = 12, h = 8, x = 0, unit = "short", max } = {}) {
  panels.push({
    id: id++, type: "timeseries", title, datasource: ds,
    gridPos: { h, w, x, y },
    fieldConfig: { defaults: { unit, ...(max !== undefined ? { max } : {}) }, overrides: [] },
    options: { legend: { displayMode: "list", placement: "bottom" }, tooltip: { mode: "multi" } },
    targets: targets.map(([expr, legendFormat], i) => ({ refId: String.fromCharCode(65 + i), expr, legendFormat, datasource: ds })),
  });
  if (x + w >= 24) y += h;
}

function stat(title, expr, { w = 6, h = 5, x = 0, unit = "short", thresholds, mappings } = {}) {
  panels.push({
    id: id++, type: "stat", title, datasource: ds,
    gridPos: { h, w, x, y },
    fieldConfig: {
      defaults: {
        unit,
        thresholds: { mode: "absolute", steps: thresholds || [{ color: "green", value: null }] },
        mappings: mappings || [],
      },
      overrides: [],
    },
    options: { colorMode: "background", reduceOptions: { calcs: ["lastNotNull"] } },
    targets: [{ refId: "A", expr, datasource: ds }],
  });
  if (x + w >= 24) y += h;
}

const notActuator = 'uri!~"/actuator.*"';

row("Correctness and SLOs");
stat("Reconciliation", "max(reconciliation_mismatch_total)", {
  x: 0,
  thresholds: [{ color: "green", value: null }, { color: "red", value: 1 }],
  mappings: [{ type: "value", options: { "0": { text: "0 mismatches" } } }],
});
stat("Last reconciliation", "max(reconciliation_last_run_timestamp_seconds) * 1000", { x: 6, unit: "dateTimeFromNow" });
stat("Balance reads < 100 ms (SLO 99%)", "flyermiles:balance_reads_under_100ms:ratio_5m", {
  x: 12, unit: "percentunit",
  thresholds: [{ color: "red", value: null }, { color: "green", value: 0.99 }],
});
stat("Earns visible < 5 s (SLO 99%)", "flyermiles:earn_within_5s:ratio_5m", {
  x: 18, unit: "percentunit",
  thresholds: [{ color: "red", value: null }, { color: "green", value: 0.99 }],
});

row("Traffic and latency");
ts("Requests per second", [[`sum by (application) (rate(http_server_requests_seconds_count{${notActuator}}[1m]))`, "{{application}}"]], { x: 0, unit: "reqps" });
ts("p95 latency by endpoint", [[`histogram_quantile(0.95, sum by (le, application, uri) (rate(http_server_requests_seconds_bucket{${notActuator}}[1m])))`, "{{application}} {{uri}}"]], { x: 12, unit: "s" });
ts("Earn latency: flight completed -> balance updated", [
  ["histogram_quantile(0.50, sum by (le) (rate(flyermiles_earn_latency_seconds_bucket[1m])))", "p50"],
  ["histogram_quantile(0.95, sum by (le) (rate(flyermiles_earn_latency_seconds_bucket[1m])))", "p95"],
  ["histogram_quantile(0.99, sum by (le) (rate(flyermiles_earn_latency_seconds_bucket[1m])))", "p99"],
], { x: 0, unit: "s" });
ts("Balance cache hit rate", [[
  'sum(rate(flyermiles_balance_cache_requests_total{result="hit"}[1m])) / sum(rate(flyermiles_balance_cache_requests_total[1m]))', "hit rate",
]], { x: 12, unit: "percentunit", max: 1 });

row("Pipeline");
ts("Events per second", [
  ["sum(rate(simulator_flights_sent_total[1m]))", "flights produced"],
  ["sum by (type) (rate(ledger_entries_appended_total[1m]))", "ledger {{type}}"],
  ['sum(rate(flyermiles_projection_events_total{result="applied"}[1m]))', "projected"],
], { x: 0, unit: "ops" });
ts("Consumer lag per group", [["sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}"]], { x: 12 });
ts("Kafka Streams: records processed per second", [["sum by (instance) (kafka_stream_thread_process_rate)", "{{instance}}"]], { x: 0, unit: "ops" });
ts("Kafka Streams: state store size", [
  ["sum by (rocksdb_window_state_id, rocksdb_state_id) (kafka_stream_state_estimate_num_keys)", "keys {{rocksdb_state_id}}{{rocksdb_window_state_id}}"],
  ["sum by (rocksdb_window_state_id, rocksdb_state_id) (kafka_stream_state_total_sst_files_size)", "sst bytes {{rocksdb_state_id}}{{rocksdb_window_state_id}}"],
], { x: 12 });

row("Ledger and scaling");
ts("Optimistic-concurrency conflicts", [
  ["sum(rate(ledger_append_conflicts_total[1m]))", "ledger conflicts/s"],
  ["sum(rate(redemption_version_conflicts_total[1m]))", "redemption retries/s"],
], { x: 0, unit: "ops" });
ts("Ledger publish backlog", [["max(ledger_publish_backlog)", "entries waiting for ledger.events"]], { x: 12 });
ts("Replicas", [
  ['sum by (deployment) (kube_deployment_status_replicas_available{namespace="flyermiles"})', "{{deployment}}"],
  ['count by (application) (up{job=~"flyermiles|earning-engine"} == 1)', "{{application}}"],
], { x: 0 });
ts("JVM heap used", [['sum by (application) (jvm_memory_used_bytes{area="heap"})', "{{application}}"]], { x: 12, unit: "bytes" });

const dashboard = {
  uid: "flyermiles",
  title: "FlyerMiles",
  tags: ["flyermiles"],
  timezone: "browser",
  refresh: "10s",
  time: { from: "now-30m", to: "now" },
  schemaVersion: 39,
  editable: true,
  panels,
};

fs.writeFileSync(path.join(__dirname, "dashboards", "flyermiles.json"), JSON.stringify(dashboard, null, 2) + "\n");
console.log(`wrote ${panels.length} panels`);
