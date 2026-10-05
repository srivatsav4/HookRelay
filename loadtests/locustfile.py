"""Locust scenarios. Run with the stack up:  make loadtest  (then open http://localhost:8089)

Select a scenario with the class picker in the UI or:  locust -f locustfile.py --host http://localhost:8000 NormalIngestion
Results are whatever YOUR machine produces; record them in docs/load-testing.md. Nothing is pre-filled.
"""
import os
import random
import uuid

from locust import HttpUser, between, constant, task

KEY = os.environ.get("DEMO_API_KEY", "hr_demo0001_demo-only-key-0123456789abcdefghijklmnop")
H = {"X-API-Key": KEY}


def post_event(user, key=None, payload=None, name="POST /events"):
    with user.client.post("/events", json={"type": "order.created", "payload": payload or {"n": random.randint(1, 10**6)}},
                          headers={**H, "Idempotency-Key": key or str(uuid.uuid4())}, name=name, catch_response=True) as r:
        if r.status_code == 202:
            r.success()
        else:
            r.failure(f"HTTP {r.status_code}")


class NormalIngestion(HttpUser):
    """Steady traffic with unique keys."""
    wait_time = between(0.05, 0.3)

    @task
    def ingest(self):
        post_event(self)


class BurstTraffic(HttpUser):
    """No think time: stresses the ingestion path, PostgreSQL pool and queue backlog."""
    wait_time = constant(0)

    @task
    def ingest(self):
        post_event(self, payload={"blob": "x" * 512})


class DuplicateTraffic(HttpUser):
    """Many users hammer a small key space: duplicates must collapse onto one event each."""
    wait_time = between(0.01, 0.1)

    @task
    def ingest(self):
        post_event(self, key=f"dup-{random.randint(0, 20)}", name="POST /events (duplicate keys)")


class FailingEndpoints(HttpUser):
    """Pair with: curl -XPOST localhost:9000/config/demo-flaky -d '{"behavior":"status","status":500}'
    Ingestion stays fast while the worker retries / opens circuits; watch the dashboard."""
    wait_time = between(0.1, 0.5)

    @task
    def ingest(self):
        post_event(self, name="POST /events (failing receivers)")


class SlowEndpoints(HttpUser):
    """Pair with: curl -XPOST localhost:9000/config/demo-ok -H 'content-type: application/json' -d '{"behavior":"slow","delay_ms":2000}'"""
    wait_time = between(0.1, 0.5)

    @task
    def ingest(self):
        post_event(self, name="POST /events (slow receivers)")
