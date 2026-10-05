# Contributing

1. `cp .env.example .env && make up` for the full stack, or `make test-integration` for tests only.
2. Pure-logic modules (`retry`, `circuit_breaker`, `state_machine`, `security/*`) must stay dependency-free and unit-tested.
3. Any change to delivery semantics needs a test in `tests/integration` or `tests/reliability` and an update to `docs/`.
4. Never log secrets; never claim exactly-once delivery; PostgreSQL stays authoritative.
5. `ruff check` must pass. Commit style: `area: imperative summary`.
