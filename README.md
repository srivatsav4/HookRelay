# HookRelay — Fault-Tolerant Event Delivery & Reliability Platform

> Reliable systems aren't proven when everything works. They're proven when everything fails.

HookRelay is a webhook/event delivery platform built around **failure handling, delivery guarantees, observability, security and
experimentally verified reliability**. Its differentiator is the **Reliability Laboratory**: a controlled environment that injects failures,
records a timeline, and checks explicit system invariants against real data.

## Demo
`docker compose up --build` → dashboard at http://localhost:3000 (login prefilled: `demo@hookrelay.local` / `demo-password`).
Screenshots are not included (none were captured in the generation environment) — run it and open **Reliability Lab → Run experiment**.

## Reliability scorecard
Honest status: invariants are *encoded as tests and lab experiments*; the evidence is produced when **you** run them.

| Invariant | Claim | Verified by | Executed in generation env? |
|---|---|---|---|
| A Durable acceptance | 202 ⇒ event in PostgreSQL | integration test, REDIS_OUTAGE lab | No (needs services) |
| B Idempotent ingestion | 100 concurrent same-key requests ⇒ 1 event | concurrency test, DUPLICATE_REQUEST lab | No |
| C At-least-once | transient failures are retried | integration + reliability tests, RECEIVER_* labs | No |
| D No concurrent ownership | one worker owns a delivery | concurrency test (25 concurrent claims) | No |
| E Recovery | Redis loss ⇒ work rebuilt from PostgreSQL | recovery tests, REDIS_OUTAGE / WORKER_CRASH labs | No |
| F Retry correctness | 4xx not retried; 5xx/429/timeout retried | unit + integration | Unit logic: **yes**; integration: no |
| G Security | SSRF blocked, keys hashed, signatures protect payload | unit/security tests, SSRF/SIGNATURE labs | Unit/security logic: **yes**; DB-backed: no |
| H Replay safety | replay = new linked delivery, history intact | integration test | No |

Executed here: 79 pure-logic checks (via a pytest shim; real pytest unavailable offline). Everything else: **Not executed in the generation environment** — see `docs/testing.md`.

## Why this exists
Most webhook demos are CRUD plus `requests.post`. This one asks: what happens when Redis dies, a worker is SIGKILLed mid-attempt, a receiver returns 429s, a tenant floods the queue, or a payload is tampered with — and *proves* the answer.

## Architecture
API → one PostgreSQL transaction (event + deliveries) → Redis job → worker → signed HTTP delivery → delivered / retry / dead-letter. PostgreSQL is authoritative; Redis is a transport; a recovery scanner re-derives lost work. Details: [docs/architecture.md](docs/architecture.md).

## Delivery semantics
**At-least-once**, never claimed exactly-once. Stable `X-HookRelay-Event-Id` lets receivers dedupe. [docs/delivery-semantics.md](docs/delivery-semantics.md).

## State machine
`PENDING → QUEUED → PROCESSING → DELIVERED | RETRYING → … | DEAD`; invalid transitions rejected; replay creates a new delivery. [docs/state-machine.md](docs/state-machine.md).

## Reliability Laboratory
12 experiment types (Redis outage, worker crash, 500/429/timeout/slow/intermittent receivers, duplicate requests, circuit breaker, signature tampering, SSRF, tenant flood). Each yields a timeline, measured metrics, invariant checks and PASS/FAIL. Fault injection is scoped (no arbitrary commands); simulated parts are labelled. [docs/reliability-lab.md](docs/reliability-lab.md).

## Failure experiments
`make chaos-redis` and `make chaos-worker` do *real* Redis-stop / worker-SIGKILL against the running stack. Catalogue: [docs/failure-scenarios.md](docs/failure-scenarios.md).

## Dashboard
Overview (events/s, success, retry rate, p50/p95/p99, dead, queue depth, workers, circuit states, recent failures), Events, Deliveries, **Delivery trace** (attempt timeline), Endpoints, Reliability Lab, Experiments, Tenants, Metrics. Static HTML/JS (no build step).

## Security
HMAC-SHA256 signing with timestamp tolerance, hashed API keys, SSRF guard (registration + every attempt), secret redaction in logs, JWT for management. Limitations are listed honestly in [docs/security.md](docs/security.md).

## Testing
`make test-unit` (no services) · `make test-integration` (real PostgreSQL + Redis). [docs/testing.md](docs/testing.md).

## Load testing
Locust scenarios in `loadtests/`. **No numbers are published** — record local results in [docs/load-testing.md](docs/load-testing.md).

## Design decisions
Why PostgreSQL is the source of truth, `SKIP LOCKED`, jitter, circuit breaking, no Kafka/Kubernetes…: [docs/design-decisions.md](docs/design-decisions.md).

## Quick start
```bash
cp .env.example .env
docker compose up --build
```
| What | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| API / docs | http://localhost:8000 · http://localhost:8000/docs |
| Mock receiver | http://localhost:9000 |
| Prometheus | http://localhost:9090 |
| Worker metrics | http://localhost:9101 |

The `seed` service creates the demo account, API key (`DEMO_API_KEY` in `.env`), three mock-receiver endpoints and 15 events sent through the real pipeline.
Demo values are for local use only (`ALLOW_PRIVATE_DESTINATIONS=true` is required so the mock receiver on the Docker network is reachable).

## API examples
```bash
KEY=hr_demo0001_demo-only-key-0123456789abcdefghijklmnop
curl -X POST localhost:8000/events -H "X-API-Key: $KEY" -H "Idempotency-Key: order-42" \
     -H 'content-type: application/json' -d '{"type":"order.created","payload":{"order_id":42}}'

TOKEN=$(curl -s localhost:8000/auth/login -H 'content-type: application/json' \
        -d '{"email":"demo@hookrelay.local","password":"demo-password"}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -H "Authorization: Bearer $TOKEN" localhost:8000/deliveries?status=DEAD
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:8000/deliveries/<id>/replay
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:8000/experiments -H 'content-type: application/json' -d '{"type":"CIRCUIT_BREAKER"}'
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:8000/experiments/<id>/start
curl localhost:8000/ready      # 503 + per-dependency detail when Postgres or Redis is down
```
Endpoints: `/auth/login`, `/endpoints` (CRUD), `/events` (+`GET`), `/deliveries` (+replay), `/experiments`, `/tenants`, `/stats/*`, `/admin/recovery`, `/health`, `/ready`, `/metrics`. Errors: `{"error":{"code","message","request_id"}}`.

## Limitations
- Never run end-to-end in the generation environment (no Docker/Postgres/Redis there): expect minor first-run fixes; CI is provided.
- Dependency versions are lower-bounded, not pinned/locked.
- Lab REDIS_OUTAGE and WORKER_CRASH are partly simulated (see docs); real chaos via scripts.
- Signing secrets stored in plaintext; no login rate limiting; single JWT secret; DNS-rebinding window between SSRF check and connect.
- Initial migration is generated from models; the lab runs one experiment at a time inside the API process.
- Percentiles use attempt durations over the last hour (PostgreSQL `percentile_cont`); the dashboard polls rather than streams.

## Future work
Envelope encryption for secrets, weighted fair scheduling, outbound egress proxy, per-endpoint pause/replay-all, OpenTelemetry traces, key rotation, locked dependencies, signed-secret rotation windows.
