#!/usr/bin/env bash
# Statement query: OFFSET vs keyset pagination, with and without the covering index, on one member
# with ~100k ledger entries. Produces EXPLAIN (ANALYZE, BUFFERS) plans for a deep page and k6 p95s.
# Usage: bench/statement-bench.sh [entries]
source "$(dirname "$0")/lib.sh"
ENTRIES=${1:-100000}
OUT=bench/results/statement-explain.txt
DEPTH=$((ENTRIES / 2))   # the "deep page" for EXPLAIN: halfway through the history

whale=$(psql_db query "SELECT member_id FROM statement_lines GROUP BY member_id HAVING count(*) >= $ENTRIES LIMIT 1")
if [[ -z "$whale" ]]; then
  whale=$(curl -sf -X POST "$MEMBER/members" -H 'Content-Type: application/json' \
    -d '{"name":"Frequent Flyer","email":"whale@example.com"}' | sed -E 's/.*"memberId":"([^"]+)".*/\1/')
  log "member $whale: producing $ENTRIES flights"
  curl -sf -X POST "$SIM/simulator/burst?count=$ENTRIES&memberId=$whale" >/dev/null
  wait_projection 900
fi
log "member $whale has $(psql_db query "SELECT count(*) FROM statement_lines WHERE member_id='$whale'") statement lines"
psql_db query "VACUUM ANALYZE statement_lines" >/dev/null

cursor=$(psql_db query "SELECT occurred_at, seq FROM statement_lines WHERE member_id='$whale'
  ORDER BY occurred_at DESC, seq DESC OFFSET $DEPTH LIMIT 1")
cur_ts=${cursor%%|*}; cur_seq=${cursor##*|}

OFFSET_SQL="SELECT seq, occurred_at, type, points, balance_after FROM statement_lines
  WHERE member_id = '$whale' ORDER BY occurred_at DESC, seq DESC OFFSET $DEPTH LIMIT 50"
KEYSET_SQL="SELECT seq, occurred_at, type, points, balance_after FROM statement_lines
  WHERE member_id = '$whale' AND (occurred_at, seq) < ('$cur_ts', $cur_seq)
  ORDER BY occurred_at DESC, seq DESC LIMIT 50"

explain() { psql_db query "EXPLAIN (ANALYZE, BUFFERS) $2" | sed "s/^/  /" | { echo "--- $1"; cat; echo; } >> "$OUT"; }
exec_ms() { psql_db query "EXPLAIN (ANALYZE, FORMAT JSON) $1" | tr -d '\n' | sed -E 's/.*"Execution Time": ([0-9.]+).*/\1/'; }

: > "$OUT"
run_k6() {
  if command -v k6 >/dev/null; then
    k6 run -q -e MODE="$1" -e MEMBER="$whale" -e LABEL="$2" -e DURATION=45s k6/statement.js | tail -8
  else
    log "k6 not installed: skipping HTTP run $2"
  fi
}

for state in noindex index; do
  if [[ $state == noindex ]]; then
    psql_db query "DROP INDEX IF EXISTS statement_lines_keyset_idx" >/dev/null
  else
    psql_db query "CREATE INDEX IF NOT EXISTS statement_lines_keyset_idx
      ON statement_lines (member_id, occurred_at DESC, seq DESC) INCLUDE (type, points, balance_after)" >/dev/null
  fi
  psql_db query "VACUUM ANALYZE statement_lines" >/dev/null
  explain "OFFSET $DEPTH, $state" "$OFFSET_SQL"
  explain "keyset at the same depth, $state" "$KEYSET_SQL"
  echo "{\"index\":\"$state\",\"depth\":$DEPTH,\"offset_exec_ms\":$(exec_ms "$OFFSET_SQL"),\"keyset_exec_ms\":$(exec_ms "$KEYSET_SQL")}" \
    | tee "bench/results/statement-sql-$state.json"
  run_k6 offset "offset-$state"
  run_k6 keyset "keyset-$state"
done
log "plans written to $OUT"
