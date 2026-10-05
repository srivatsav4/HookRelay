# Design decisions

- **PostgreSQL is the source of truth; Redis is a transport.** Redis can be flushed or down; the system must stay correct. Acceptance, state, history and retry schedule live in PostgreSQL; Redis only wakes workers up.
- **At-least-once, not exactly-once.** See delivery-semantics.md. Claiming exactly-once over HTTP would be dishonest; we give stable ids so receivers dedupe.
- **Idempotency via a DB unique constraint + `ON CONFLICT DO NOTHING`.** Check-then-insert races under concurrency; the constraint is the only arbiter that cannot race.
- **Commit before enqueue.** The reverse order can lose or ghost events. If the enqueue fails after commit, recovery repairs it from PostgreSQL.
- **Row locking with `FOR UPDATE SKIP LOCKED` on claim.** Gives mutual exclusion without blocking: a competing worker skips instead of waiting. The status check inside the lock makes claims idempotent.
- **Exponential backoff with jitter.** Fixed schedules synchronise retries after an outage into a thundering herd; ±20% jitter spreads them. A budget bounds total retry time.
- **Release without consuming an attempt** (circuit open / rate limit / tenant cap): these are *our* decisions, not receiver failures, and must not push a healthy event to DEAD.
- **Circuit breaker per endpoint, state in PostgreSQL.** Prevents a dead receiver from soaking up worker slots and retries; survives worker restarts; one HALF_OPEN probe prevents a stampede on recovery. 429/4xx don't count (the endpoint is alive).
- **Endpoint isolation.** Breaker, rate limit, retry schedule and timeout are per endpoint so one bad receiver cannot degrade others.
- **Tenant fairness = per-tenant concurrency cap (Redis counter).** Simple to explain and test; cost: a leaked counter after a crash self-heals via TTL. Not a full weighted fair scheduler — that would be over-engineering here.
- **Deterministic arq job ids** make enqueue idempotent, so the recovery scanner can blindly re-enqueue without creating duplicate jobs.
- **Stale-lock recovery counts the crashed attempt as consumed.** It may have reached the receiver; being conservative keeps the attempt budget honest.
- **Docker Compose, not Kubernetes.** The point is delivery semantics, not orchestration; Compose runs the whole demo with one command. **No Kafka/RabbitMQ:** the workload is per-delivery scheduling with delays and
  retries, which PostgreSQL + a lightweight Redis queue handle; a log broker would add operational weight without improving the guarantees.
- **Lab fault injection is scoped** (mock receiver API, an enqueue-path gate, DB-state simulation); real process chaos is an operator script. Simulated parts are labelled as such.
- **Initial migration uses `metadata.create_all`.** Pragmatic for revision 0001; subsequent revisions must be explicit.
