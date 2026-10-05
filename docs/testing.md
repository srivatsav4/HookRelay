# Testing

| Suite | Needs | What it proves |
|---|---|---|
| `tests/unit` | nothing | HMAC, retry classification/schedule/jitter bounds/budget, circuit breaker, state machine, API-key & password hashing, log redaction |
| `tests/security` | nothing | SSRF matrix, signature tampering/replay |
| `tests/integration` | PostgreSQL + Redis | ingestion API, auth, delivery, retries, DLQ, replay (Invariant H), recovery (E), circuit, rate limit, API key not stored in plaintext (G) |
| `tests/concurrency` | PostgreSQL + Redis | 100 concurrent same-key requests → 1 event (B); 25 concurrent claims → 1 owner (D); duplicate jobs deliver once; tenant cap |
| `tests/reliability` | PostgreSQL + Redis | receiver outage→recovery, timeouts, Redis outage + worker crash combined, stable event id across retries |

Run: `make test-unit` (no services) · `make test-integration` (starts Postgres/Redis via `docker-compose.test.yml`). Integration suites auto-skip without `DATABASE_URL`/`REDIS_URL`.

## Execution status in the generation environment
- `tests/unit` + `tests/security` logic: **executed** (79 checks passed) using a minimal pytest-compatible shim because pytest could not be installed (no network). Not run under real pytest.
- `tests/integration`, `tests/concurrency`, `tests/reliability`, the Reliability Lab, Docker build, Alembic migration, Locust: **Not executed in the generation environment.** Expect to fix small
  integration issues on first run; CI (`.github/workflows/ci.yml`) runs all of them.
- All Python compiled with `py_compile`; `frontend/app.js` passed `node --check`.
