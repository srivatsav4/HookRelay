#!/usr/bin/env sh
# REAL worker crash: SIGKILL the worker container mid-traffic, then restart it.
set -eu
echo "[chaos] killing worker"; docker compose kill -s KILL worker
sleep 3; echo "[chaos] restarting worker"; docker compose start worker
echo "[chaos] deliveries left PROCESSING are reclaimed after VISIBILITY_TIMEOUT_S by the recovery scanner"
