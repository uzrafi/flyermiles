// Balance reads at a fixed arrival rate over a pool of members.
//   k6 run -e RATE=500 -e DURATION=60s -e LABEL=cache-on k6/balance-read.js
// The query-service cache is toggled by bench/cache-bench.sh (POST /admin/cache?enabled=...).
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const QUERY = __ENV.QUERY_URL || "http://localhost:8084";
const MEMBERS = __ENV.MEMBER_URL || "http://localhost:8081";
const LABEL = __ENV.LABEL || "run";
const HOT = Number(__ENV.HOT_MEMBERS || 2000); // members drawn from; a realistic app traffic working set

const sources = new Counter("balance_source");

export const options = {
  discardResponseBodies: false,
  scenarios: {
    reads: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 500),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  thresholds: { http_req_failed: ["rate<0.01"] },
  summaryTrendStats: ["avg", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export function setup() {
  const ids = http.get(`${MEMBERS}/members/ids?limit=${HOT}`).json();
  if (!ids.length) throw new Error("no members: run bench/seed.sh first");
  // Warm-up pass so the cache-on run measures steady state, not cold misses.
  if (__ENV.WARM === "true") ids.forEach((id) => http.get(`${QUERY}/members/${id}/balance`));
  return { ids };
}

export default function (data) {
  const id = data.ids[Math.floor(Math.random() * data.ids.length)];
  const res = http.get(`${QUERY}/members/${id}/balance`, { tags: { name: "balance" } });
  check(res, { "200": (r) => r.status === 200 });
  if (res.status === 200) sources.add(1, { source: res.json("source") });
}

export function handleSummary(data) {
  const d = data.metrics.http_req_duration.values;
  const out = {
    label: LABEL,
    requests: data.metrics.http_reqs.values.count,
    rps: Math.round(data.metrics.http_reqs.values.rate),
    p50_ms: +d.med.toFixed(2),
    p95_ms: +d["p(95)"].toFixed(2),
    p99_ms: +d["p(99)"].toFixed(2),
    failed_rate: data.metrics.http_req_failed.values.rate,
  };
  return {
    stdout: JSON.stringify(out, null, 2) + "\n",
    [`bench/results/balance-${LABEL}.json`]: JSON.stringify(out, null, 2),
  };
}
