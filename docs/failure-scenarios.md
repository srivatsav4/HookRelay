# Failure scenarios & expected behaviour

| Failure | What happens | Invariant |
|---|---|---|
| Redis down at ingest | event + deliveries committed; enqueue fails; rows stay PENDING; `/ready` → 503; recovery re-enqueues after Redis returns | A, E |
| Redis loses all data | QUEUED rows older than `RECOVERY_QUEUED_GRACE_S` are re-enqueued (deterministic job ids dedupe) | E |
| Worker dies mid-attempt | row stays PROCESSING; after `VISIBILITY_TIMEOUT_S` recovery sets RETRYING (or DEAD if attempts exhausted); receiver may see a duplicate | C |
| Two workers get the same job | `FOR UPDATE SKIP LOCKED` + status check: one claims, the other skips | D |
| Receiver 5xx / timeout / 429 | classified transient → backoff+jitter (Retry-After honoured) → retry | C, F |
| Receiver 404/400 | permanent → DEAD immediately; replay available | F |
| Receiver hard-down | breaker opens after N failures; deliveries are released (attempt not consumed) until the cooldown probe succeeds | – |
| Receiver slow | per-attempt timeout; bounded worker concurrency; tenant cap prevents pool monopolisation | – |
| Duplicate submissions | unique constraint collapses them to one event | B |
| Tampered/replayed webhook | receiver verification fails (see `examples/verify_signature.py`) | G |
| Private/loopback destination | rejected at registration and again at send time | G |
| Dead delivery replayed | new delivery linked by `replay_of`; history & audit preserved | H |
| API process restarts mid-experiment | experiment marked FAILED on next start | – |
Crash between the HTTP call and the finalize commit is the unavoidable at-least-once window: HookRelay will redeliver.
