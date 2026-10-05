# Load testing

`loadtests/locustfile.py`: `NormalIngestion`, `BurstTraffic`, `DuplicateTraffic`, `FailingEndpoints`, `SlowEndpoints`. Start the stack, then `make loadtest` and open http://localhost:8089.
For failing/slow scenarios, flip the receiver first (commands are in each class docstring).

**No benchmark numbers are included because none were measured.** Record your own below; they are local results, not claims about production capacity.

| Date | Machine (CPU/RAM/OS) | Scenario | Users | Duration | Req/s | p50 / p95 / p99 | Failures | Notes |
|---|---|---|---|---|---|---|---|---|
| _unmeasured_ | | | | | | | | |

After a run verify: duplicates collapsed (`SELECT count(*) FROM events` vs distinct keys), backlog drained (dashboard Overview), no `DEAD` you did not cause.
Single API process, pool 10+10 connections: ingestion throughput is bounded by PostgreSQL commit latency and that pool; scale with more API replicas/pool before tuning code.
