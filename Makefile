.PHONY: up down logs test-unit test-integration test-all lint seed loadtest chaos-redis chaos-worker zip

up:            ## build and start everything
	@test -f .env || cp .env.example .env
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f api worker

test-unit:     ## pure-logic tests, no services needed
	cd backend && python -m pytest tests/unit tests/security -q

test-services: ## start only Postgres + Redis for integration tests
	docker compose -f docker-compose.test.yml up -d --wait

test-integration: test-services
	cd backend && DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@localhost:55432/hookrelay_test \
	  REDIS_URL=redis://localhost:56379/1 JWT_SECRET=test-jwt-secret-0123456789 API_KEY_PEPPER=test-pepper-0123456789 \
	  python -m pytest tests -q

test-all: test-integration

lint:
	cd backend && ruff check app tests

seed:
	docker compose run --rm seed

loadtest:      ## requires the stack to be running; see docs/load-testing.md
	cd loadtests && locust -f locustfile.py --host http://localhost:8000

chaos-redis:   ## REAL Redis outage (operator action, local demo only)
	./scripts/chaos_redis.sh

chaos-worker:  ## REAL worker kill
	./scripts/chaos_worker.sh

zip:
	./scripts/package.sh
