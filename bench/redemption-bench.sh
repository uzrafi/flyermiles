#!/usr/bin/env bash
# N redemptions at once against one member over HTTP (redemption-service -> ledger-service). The member
# is funded for exactly half of them. Afterwards the ledger is checked directly for overdrafts.
# Usage: bench/redemption-bench.sh [N]
source "$(dirname "$0")/lib.sh"
N=${1:-1000}
POINTS=100

member=$(curl -sf -X POST "$MEMBER/members" -H 'Content-Type: application/json' \
  -d '{"name":"Stampede Test","email":"stampede@example.com"}' | sed -E 's/.*"memberId":"([^"]+)".*/\1/')
# Fund through the real earn path until the ledger balance is at least N/2 * POINTS, then note it.
target=$((N / 2 * POINTS))
log "funding $member to >= $target points"
while (( $(curl -sf "$LEDGER/ledger/members/$member" | sed -E 's/.*"balance":([0-9]+).*/\1/') < target )); do
  curl -sf -X POST "$SIM/simulator/burst?count=2&memberId=$member" >/dev/null; sleep 1
done
sleep 3
start=$(curl -sf "$LEDGER/ledger/members/$member" | sed -E 's/.*"balance":([0-9]+).*/\1/')
log "starting balance $start; firing $N concurrent redemptions of $POINTS"

k6 run -q -e N="$N" -e MEMBER="$member" -e POINTS="$POINTS" -e RUN_ID="$(date +%s)" k6/redemptions.js | tail -8

overdrafts=$(psql_db ledger "SELECT count(*) FROM ledger_entries WHERE member_id='$member' AND balance_after < 0")
redeemed=$(psql_db ledger "SELECT COALESCE(-sum(points),0) FROM ledger_entries WHERE member_id='$member' AND type='REDEEMED'")
final=$(curl -sf "$LEDGER/ledger/members/$member" | sed -E 's/.*"balance":([0-9]+).*/\1/')
echo "{\"concurrent\":$N,\"startingBalance\":$start,\"redeemed\":$redeemed,\"finalBalance\":$final,\"overdrafts\":$overdrafts,\"maxPossible\":$(( start / POINTS * POINTS ))}" \
  | tee "bench/results/redemption-ledger-$N.json"
