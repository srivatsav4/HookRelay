# Delivery state machine

```
PENDING ──enqueue ok──▶ QUEUED ──claim──▶ PROCESSING ──2xx──────────────▶ DELIVERED (terminal)
   │                                        │  ▲  └──transient, attempts left──▶ RETRYING ──claim──┘
   └─────────claim (job from recovery)──────┘  │  └──permanent / exhausted──────▶ DEAD (terminal)
                      release (circuit open / rate limited / tenant cap): PROCESSING ─▶ RETRYING, attempt NOT consumed
                      stale lock (worker died): PROCESSING ─▶ RETRYING | DEAD (set by recovery)
```
Allowed: PENDING→{QUEUED,PROCESSING}; QUEUED→PROCESSING; PROCESSING→{DELIVERED,RETRYING,DEAD}; RETRYING→PROCESSING. Terminal states have no exits;
`replay` creates a **new** delivery (`replay_of`), so history is never rewritten. `app/services/state_machine.py` raises `InvalidTransition`;
a DB `CHECK` constrains the status values. Tests: `tests/unit/test_state_machine.py`.

Circuit breaker (per endpoint): CLOSED →(N consecutive transient failures; 429 and 4xx don't count)→ OPEN →(cooldown, one probe)→ HALF_OPEN →
success→CLOSED / failure→OPEN. DEGRADED is a derived label (CLOSED with failures>0).
