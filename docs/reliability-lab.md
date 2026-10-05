# Reliability Laboratory

`POST /experiments {type, config}` → `POST /experiments/{id}/start` → `GET /experiments/{id}` (timeline, invariants, metrics, verdict).
Runs in the API process, one at a time, in isolated *lab tenants* (`is_lab`) pointed at the mock receiver. Every number in a result is
read back from PostgreSQL / the receiver's counters after the run; the verdict is PASS only if **all** invariants pass. A run that
cannot finish (e.g. worker not running) times out and reports FAIL — it never fabricates success.

## What is injected (and what is honestly simulated)
| Type | Injection | Real or simulated |
|---|---|---|
| RECEIVER_500 / 429 / TIMEOUT / SLOW / INTERMITTENT | mock receiver behaviour API | real HTTP behaviour |
| DUPLICATE_REQUEST | N concurrent ingests, one key | real DB contention |
| CIRCUIT_BREAKER | receiver hard-down, then healed | real |
| SIGNATURE_TAMPERING | tampered/wrong-secret/stale requests to a verifying receiver | real |
| SSRF_ATTEMPT | 15 hostile URLs through the validator | real |
| TENANT_FLOOD | one tenant floods a slow receiver | real |
| REDIS_OUTAGE | in-process gate makes the API's **enqueue path** fail | **simulated** at the enqueue boundary; the worker's Redis stays up, so the scanner may recover work before the gate lifts |
| WORKER_CRASH | DB rows set to PROCESSING with an expired lock | **simulated post-crash state** |

For *real* process-level chaos use `make chaos-redis` (stops the Redis container while ingesting) and `make chaos-worker` (SIGKILL the worker).
These are operator scripts, not exposed through the web UI. The UI can only start the fixed experiment types above — no arbitrary commands.

## Experiment result shape
`experiment_id, type, config, started_at, ended_at, duration_s, injected_failure, metrics{events_generated, events_accepted, events_in_db, deliveries_*,
attempts_total, retries, recovery_actions, duplicate_requests, latency_ms{p50,p95,max}, …}, invariants[{invariant,name,passed,detail}], verdict`.

Status: the lab has **not been executed** in the generation environment (no Docker/Postgres/Redis available there). Run it locally and read the results.
