# Delivery semantics

| Guarantee | Meaning | Achievable over arbitrary HTTP receivers? |
|---|---|---|
| At-most-once | never retry; may lose events | yes, but unacceptable for events that matter |
| **At-least-once** | retry until acknowledged; may duplicate | **yes — what HookRelay does** |
| Exactly-once | each event processed once, no loss, no duplicate | no (see below) |

**Why not exactly-once.** The sender cannot distinguish "receiver processed the request but the response was lost" from
"receiver never got it" (Two Generals problem). Retrying risks a duplicate; not retrying risks a loss. Exactly-once *effects*
require the receiver to participate: deduplicate on a stable id. HookRelay therefore guarantees *at-least-once delivery*
and gives receivers the tools for *effectively-once processing*: a stable `X-HookRelay-Event-Id` (also `id` in the body)
that is identical across attempts, a stable `X-HookRelay-Delivery-Id`, and `X-HookRelay-Attempt`.

**Where duplicates come from here:** timeouts after the receiver processed the request; a worker dying after the HTTP call but
before the finalize commit (the stale-PROCESSING reclaim retries it); replays; operator re-delivery.

**What ingestion guarantees** is separate: with `Idempotency-Key`, an event is accepted *exactly once logically* (DB unique
constraint). A 202 means the event is durable in PostgreSQL (Invariant A).

Retry policy: 10s, 30s, 2m, 10m, 1h (configurable `RETRY_SCHEDULE`) ±20% jitter ⇒ 6 attempts; budget 24h; `Retry-After` honoured (capped).
Transient: timeout, connection error, 408/425/429, 5xx. Permanent (not retried): other 4xx, 3xx (redirects are never followed).
Receiver reference: `examples/verify_signature.py`.
