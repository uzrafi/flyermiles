#!/usr/bin/env bash
# Drops the read model and replays it from ledger.events, then reconciles.
source "$(dirname "$0")/lib.sh"

entries=$(psql_db ledger "SELECT count(*) FROM ledger_entries")
log "rebuilding read model from $entries ledger events"
curl -sf -X POST "$QUERY/admin/rebuild" >/dev/null
until curl -sf "$QUERY/admin/rebuild" | grep -q '"complete":true'; do sleep 1; done
curl -sf "$QUERY/admin/rebuild" | tee bench/results/rebuild.json; echo
docker compose run --rm reconciliation-job 2>&1 | sed -n '/RECONCILIATION/,$p' > bench/results/reconciliation-after-rebuild.txt
grep -E '"ok"|"mismatches"' bench/results/reconciliation-after-rebuild.txt
