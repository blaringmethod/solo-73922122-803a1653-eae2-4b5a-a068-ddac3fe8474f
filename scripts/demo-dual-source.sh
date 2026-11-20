#!/usr/bin/env bash
# End-to-end demo of the dual-source (primary / backup gateway) hourly ingest:
#   backup fills an hour -> primary fills it later -> primary takes over ->
#   retransmission is a no-op -> conflicting retransmission is rejected ->
#   query shows the serving source and its detail.
# Run against a live stack: ./scripts/demo-dual-source.sh
# Override BASE / HOUR (whole UTC hour) if you like.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
HOUR="${HOUR:-$(date -u +%Y-%m-%dT%H:00:00Z)}"
IX="${IX:-IX-DEMO}"
PREV_HOUR=$(date -u -d "$HOUR - 1 hour" +%Y-%m-%dT%H:00:00Z 2>/dev/null || \
            date -u -j -v-1H -f "%Y-%m-%dT%H:%M:%SZ" "$HOUR" +%Y-%m-%dT%H:00:00Z)

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
need curl; need jq

wait_api() {
  for _ in $(seq 1 60); do
    curl -fsS "$BASE/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  fail "API is not reachable at $BASE"
}

q() { # q <intersection> <hour>
  curl -sS -G "$BASE/ingest/hours" \
    --data-urlencode "intersection_id=$1" --data-urlencode "hour_utc=$2"
}

say "0. wait for the API"
wait_api
if [ "${RESET:-1}" = "1" ]; then
  curl -fsS -X POST "$BASE/demo/reset?confirm=true" | jq -r '"reset business + ingest data: \(.reset)"'
fi
echo "hour under test: $HOUR   intersection: $IX"

say "1. backup gateway reports the first 40 minutes (not ready yet)"
curl -fsS -X POST "$BASE/demo/dual-source/generate" \
  -G --data-urlencode "intersection_id=$IX" \
     --data-urlencode "source=backup" \
     --data-urlencode "hour_utc=$HOUR" \
     --data-urlencode "start_minute=0" \
     --data-urlencode "end_minute=39" | jq '{source, new_minutes, ready, progress: .progress.backup}'

code=$(curl -sS -o /tmp/ds_not_ready.json -w '%{http_code}' \
  -G "$BASE/ingest/hours" \
  --data-urlencode "intersection_id=$IX" --data-urlencode "hour_utc=$HOUR")
[ "$code" = "409" ] || fail "expected 409 (not ready) while only 40/60 minutes are in, got $code"
jq -r '.detail.message' /tmp/ds_not_ready.json

say "2. backup reports its remaining minutes; backup now serves the hour"
curl -fsS -X POST "$BASE/demo/dual-source/generate" \
  -G --data-urlencode "intersection_id=$IX" \
     --data-urlencode "source=backup" \
     --data-urlencode "hour_utc=$HOUR" \
     --data-urlencode "start_minute=40" \
     --data-urlencode "end_minute=59" | jq '{new_minutes, ready, serving_source, version}'
q "$IX" "$HOUR" | jq '{ready, serving_source, version, row_count, first: .rows[0]}'

say "3. primary gateway reports all 60 minutes, marking 2 of them missing"
curl -fsS -X POST "$BASE/demo/dual-source/generate" \
  -G --data-urlencode "intersection_id=$IX" \
     --data-urlencode "source=primary" \
     --data-urlencode "hour_utc=$HOUR" \
     --data-urlencode "start_minute=0" \
     --data-urlencode "end_minute=59" \
     --data-urlencode "missing_start=10" \
     --data-urlencode "missing_end=11" | jq '{ready, serving_source, version, published}'
hour_json=$(q "$IX" "$HOUR")
echo "$hour_json" | jq '{serving_source, version, row_count, missing_minutes, first: .rows[0]}'

say "4. assertions: primary took over; its missing minutes dropped the backup readings"
serving=$(echo "$hour_json" | jq -r .serving_source)
rows=$(echo "$hour_json" | jq -r .row_count)
missing=$(echo "$hour_json" | jq -r '.missing_minutes | length')
first_count=$(echo "$hour_json" | jq -r '.rows[0].vehicle_count')
[ "$serving" = "primary" ] || fail "expected primary to serve the hour, got $serving"
[ "$rows" = "58" ] || fail "expected 58 rows (60 minus 2 missing), got $rows"
[ "$missing" = "2" ] || fail "expected 2 missing minutes, got $missing"
[ "$first_count" -lt 100 ] || fail "row still holds the backup reading ($first_count); expected the primary's"

say "5. identical retransmission changes nothing (same version, no new rows)"
before_version=$(echo "$hour_json" | jq -r .version)
curl -fsS -X POST "$BASE/demo/dual-source/generate" \
  -G --data-urlencode "intersection_id=$IX" \
     --data-urlencode "source=primary" \
     --data-urlencode "hour_utc=$HOUR" \
     --data-urlencode "start_minute=0" \
     --data-urlencode "end_minute=59" \
     --data-urlencode "missing_start=10" \
     --data-urlencode "missing_end=11" | jq '{new_minutes, changed, version}'
after_version=$(q "$IX" "$HOUR" | jq -r .version)
[ "$before_version" = "$after_version" ] || fail "retransmission bumped the version"

say "6. a conflicting retransmission of the same minute is rejected as a batch"
code=$(curl -sS -o /tmp/ds_conflict.json -w '%{http_code}' -X POST "$BASE/ingest/reports" \
  -H 'Content-Type: application/json' \
  -d "{\"intersection_id\":\"$IX\",\"source\":\"primary\",\"hour_utc\":\"$HOUR\",
       \"minutes\":[{\"minute_utc\":\"$HOUR\",\"vehicle_count\":9999}]}")
echo "HTTP $code"; jq '.detail // .' /tmp/ds_conflict.json | head -20
[ "$code" = "409" ] || fail "expected 409 for a conflicting minute, got $code"

say "7. a late backup retransmission cannot pull the hour back to backup"
curl -fsS -X POST "$BASE/demo/dual-source/generate" \
  -G --data-urlencode "intersection_id=$IX" \
     --data-urlencode "source=backup" \
     --data-urlencode "hour_utc=$HOUR" \
     --data-urlencode "start_minute=0" \
     --data-urlencode "end_minute=0" | jq '{ready, serving_source, version, published}'
q "$IX" "$HOUR" | jq -r .serving_source | grep -qx primary || fail "hour reverted to backup"

say "8. progress endpoint shows both gateways' upload state"
curl -fsS -G "$BASE/ingest/progress" \
  --data-urlencode "intersection_id=$IX" --data-urlencode "hour_utc=$HOUR" \
  | jq '{ready, serving_source, progress}'

say "9. an hour nobody reported is still not ready"
code=$(curl -sS -o /tmp/ds_prev.json -w '%{http_code}' -G "$BASE/ingest/hours" \
  --data-urlencode "intersection_id=$IX" --data-urlencode "hour_utc=$PREV_HOUR")
[ "$code" = "409" ] || fail "expected 409 for an unreported hour, got $code"
jq -r '.detail.message' /tmp/ds_prev.json

printf '\n\033[32mDEMO OK: backup served first, the primary took the hour over, the primary'"'"'s missing minutes removed the backup readings, retransmissions were idempotent, conflicts were rejected and the hour never reverted.\033[0m\n'
