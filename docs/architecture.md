# Architecture

```
Client ──POST /events (API key + Idempotency-Key)──▶ API (FastAPI)
                                                      │ 1. validate
                                                      │ 2. ONE PostgreSQL transaction: INSERT event ON CONFLICT DO NOTHING
                                                      │    + one PENDING delivery per matching endpoint  ── COMMIT = durable
                                                      │ 3. enqueue arq job (deterministic job id)  ── may fail safely
                                                      ▼
                                   Redis (arq queue)  ◀── transport only, never authoritative
                                                      │
                                                      ▼
Worker (bounded concurrency) ─ claim (FOR UPDATE SKIP LOCKED) ─▶ gates: tenant cap → circuit breaker → rate limit
        │                                                         ▼
        │                                           signed HTTP POST (no redirects, per-attempt SSRF re-check)
        │                                                         ▼
        └── finalize in one tx: attempt row + DELIVERED | RETRYING(next_attempt_at, re-enqueue deferred) | DEAD
Recovery scanner (arq cron, every RECOVERY_INTERVAL_S, also POST /admin/recovery):
        stale PROCESSING → RETRYING/DEAD;  PENDING/QUEUED/RETRYING with no job → re-enqueue (idempotent job ids)
```

Processes: `api`, `worker` (+ recovery cron + heartbeat), `mock-receiver`, `frontend` (static), `postgres`, `redis`, `prometheus`.

Key properties: acceptance is decided by PostgreSQL alone; every Redis interaction is recoverable; every state change goes
through the delivery state machine; every attempt is a row (`delivery_attempts`).

Tables: accounts, users, api_keys, endpoints, events, deliveries, delivery_attempts, experiments, experiment_events, audit_logs.
Indexes: unique `(account_id, idempotency_key)`; `(status, next_attempt_at)`; `(account_id, status)`; unique `(delivery_id, attempt_number)`.

Job ids: `{delivery_id}:{attempt_count}:{release_count}`. Same logical job ⇒ same id ⇒ enqueue is a no-op (recovery is idempotent).
`release_count` exists because arq refuses an id whose job key still exists; a released delivery needs a fresh id.
