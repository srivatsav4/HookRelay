#!/usr/bin/env sh
# REAL Redis outage for the local demo: stop Redis, keep sending events, restart, watch recovery.
set -eu
KEY="${DEMO_API_KEY:-hr_demo0001_demo-only-key-0123456789abcdefghijklmnop}"
echo "[chaos] stopping redis"; docker compose stop redis
for i in $(seq 1 10); do
  curl -s -o /dev/null -w "ingest $i -> HTTP %{http_code}\n" -X POST http://localhost:8000/events \
    -H "X-API-Key: $KEY" -H "Idempotency-Key: chaos-redis-$(date +%s)-$i" -H 'Content-Type: application/json' \
    -d '{"type":"chaos.redis","payload":{"i":'"$i"'}}'
done
echo "[chaos] /ready while Redis is down:"; curl -s http://localhost:8000/ready; echo
echo "[chaos] starting redis"; docker compose start redis
echo "[chaos] the recovery scanner should re-enqueue the PENDING deliveries within ~RECOVERY_INTERVAL_S seconds"
