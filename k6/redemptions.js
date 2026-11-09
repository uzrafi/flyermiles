// N redemptions fired at ONE member at the same moment, through the real HTTP path
// (redemption-service -> ledger-service). The balance covers only half of them.
//   k6 run -e N=500 -e MEMBER=<uuid> -e POINTS=100 k6/redemptions.js
// A 503 means "gave up under contention, safe to retry with the same Idempotency-Key", so we do.
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";
import exec from "k6/execution";

const REDEEM = __ENV.REDEMPTION_URL || "http://localhost:8083";
const N = Number(__ENV.N || 500);
const accepted = new Counter("redemptions_accepted");
const rejected = new Counter("redemptions_insufficient");
const retried = new Counter("redemptions_retried_503");

export const options = {
  scenarios: {
    stampede: { executor: "per-vu-iterations", vus: N, iterations: 1, maxDuration: "5m" },
  },
  summaryTrendStats: ["avg", "med", "p(95)", "p(99)", "max"],
};

export default function () {
  const key = `k6-${__ENV.RUN_ID || "run"}-${exec.vu.idInTest}`;
  const body = JSON.stringify({ memberId: __ENV.MEMBER, points: Number(__ENV.POINTS || 100), rewardId: "lounge-pass" });
  const params = { headers: { "Content-Type": "application/json", "Idempotency-Key": key }, tags: { name: "redeem" } };
  for (let attempt = 0; attempt < 50; attempt++) {
    const res = http.post(`${REDEEM}/redemptions`, body, params);
    if (res.status === 503) {
      retried.add(1);
      sleep(Math.random() * 0.05);
      continue;
    }
    check(res, { "201 or 422": (r) => r.status === 201 || r.status === 422 });
    if (res.status === 201) accepted.add(1);
    if (res.status === 422) rejected.add(1);
    return;
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const out = {
    concurrent: N,
    accepted: m.redemptions_accepted ? m.redemptions_accepted.values.count : 0,
    rejected_insufficient: m.redemptions_insufficient ? m.redemptions_insufficient.values.count : 0,
    retried_503: m.redemptions_retried_503 ? m.redemptions_retried_503.values.count : 0,
    p95_ms: +m.http_req_duration.values["p(95)"].toFixed(2),
  };
  return {
    stdout: JSON.stringify(out, null, 2) + "\n",
    [`bench/results/redemptions-${N}.json`]: JSON.stringify(out, null, 2),
  };
}
