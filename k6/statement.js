// Statement pages at random depths for one member with ~100k entries.
//   k6 run -e MODE=offset -e MEMBER=<uuid> -e LABEL=offset-noindex k6/statement.js
//   k6 run -e MODE=keyset -e MEMBER=<uuid> -e LABEL=keyset-index   k6/statement.js
// For keyset, setup() walks the whole statement once to collect a cursor for every page, so both
// modes fetch the same random pages and the comparison is apples to apples.
import http from "k6/http";
import { check } from "k6";

const QUERY = __ENV.QUERY_URL || "http://localhost:8084";
const MODE = __ENV.MODE || "keyset";
const MEMBER = __ENV.MEMBER;
const LIMIT = 50;
const LABEL = __ENV.LABEL || MODE;

export const options = {
  setupTimeout: "10m",
  scenarios: {
    pages: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 10),
      duration: __ENV.DURATION || "60s",
    },
  },
  // A threshold on the tagged sub-metric makes k6 report page requests separately from the setup walk.
  thresholds: { "http_req_duration{name:page}": ["p(95)>=0"] },
  summaryTrendStats: ["avg", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export function setup() {
  if (!MEMBER) throw new Error("MEMBER is required");
  const cursors = [null];
  let cursor = null;
  // Walk the statement once; also tells us how many pages there are.
  for (;;) {
    const url = `${QUERY}/members/${MEMBER}/statement?limit=${LIMIT}` + (cursor ? `&cursor=${cursor}` : "");
    const page = http.get(url, { tags: { name: "setup" } }).json();
    cursor = page.nextCursor;
    if (!cursor) break;
    cursors.push(cursor);
  }
  return { cursors };
}

export default function (data) {
  const page = Math.floor(Math.random() * data.cursors.length);
  let res;
  if (MODE === "offset") {
    res = http.get(`${QUERY}/members/${MEMBER}/statement-offset?page=${page}&limit=${LIMIT}`, { tags: { name: "page" } });
  } else {
    const c = data.cursors[page];
    res = http.get(`${QUERY}/members/${MEMBER}/statement?limit=${LIMIT}` + (c ? `&cursor=${c}` : ""), { tags: { name: "page" } });
  }
  check(res, { "200": (r) => r.status === 200 });
}

export function handleSummary(data) {
  const d = data.metrics["http_req_duration{name:page}"] || data.metrics.http_req_duration;
  const v = d.values;
  const out = {
    label: LABEL, mode: MODE,
    pages: (data.setup_data && data.setup_data.cursors.length) || null,
    requests: data.metrics.iterations.values.count,
    p50_ms: +v.med.toFixed(2), p95_ms: +v["p(95)"].toFixed(2), p99_ms: +v["p(99)"].toFixed(2),
  };
  return {
    stdout: JSON.stringify(out, null, 2) + "\n",
    [`bench/results/statement-${LABEL}.json`]: JSON.stringify(out, null, 2),
  };
}
