from prometheus_client import Counter, Gauge, Histogram

EVENTS = Counter("hookrelay_events_total", "Ingestion results", ["result"])
ATTEMPTS = Counter("hookrelay_delivery_attempts_total", "Delivery attempts", ["outcome"])
ATTEMPT_SECONDS = Histogram(
    "hookrelay_delivery_attempt_seconds", "Delivery attempt duration",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
RETRIES = Counter("hookrelay_retries_scheduled_total", "Retries scheduled")
DEAD = Counter("hookrelay_deliveries_dead_total", "Deliveries moved to DEAD")
RECOVERED = Counter("hookrelay_recovery_reenqueued_total", "Jobs re-enqueued by recovery", ["reason"])
CB_TRANSITIONS = Counter("hookrelay_circuit_transitions_total", "Circuit transitions", ["transition"])
RATE_LIMITED = Counter("hookrelay_rate_limited_total", "Deliveries deferred by endpoint rate limit")
TENANT_THROTTLED = Counter("hookrelay_tenant_throttled_total", "Deliveries deferred by tenant fairness cap")
BACKLOG = Gauge("hookrelay_deliveries_by_status", "Deliveries by status (sampled at scrape)", ["status"])
QUEUE_DEPTH = Gauge("hookrelay_redis_queue_depth", "Jobs in the arq Redis queue (sampled at scrape)")
