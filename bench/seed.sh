#!/usr/bin/env bash
# Enrols members (default 10k) and gives each a little history, so reads hit realistic data.
source "$(dirname "$0")/lib.sh"
N=${1:-10000}

existing=$(psql_db members "SELECT count(*) FROM members")
if (( existing < N )); then
  log "enrolling $((N - existing)) members"
  todo=$((N - existing))
  while (( todo > 0 )); do
    batch=$(( todo > 5000 ? 5000 : todo ))
    curl -sf -X POST "$MEMBER/members/bulk" -H 'Content-Type: application/json' -d "{\"count\":$batch}" >/dev/null
    todo=$((todo - batch))
  done
fi
curl -sf -X POST "$SIM/simulator/members/reload?limit=$N" >/dev/null
log "producing $((N * 10)) flights so every member has a balance"
curl -sf -X POST "$SIM/simulator/burst?count=$((N * 10))" && echo
wait_projection 600
