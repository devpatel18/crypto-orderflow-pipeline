.PHONY: up down clean ps logs smoke venv test lint topics producer check-gaps

venv:
	python3.11 -m venv .venv
	.venv/bin/pip install -q -e '.[dev]'

test:
	.venv/bin/pytest -q

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

# Explicit creation (not broker auto-create) so partition counts are deliberate.
# book.l2.raw needs a raised message limit: full L2 snapshots exceed 1MB.
topics:
	docker exec redpanda rpk topic create trades.raw -p 3 -r 1 || true
	docker exec redpanda rpk topic create book.l2.raw -p 3 -r 1 -c max.message.bytes=10485760 || true
	docker exec redpanda rpk topic create dlq.malformed -p 1 -r 1 || true
	docker exec redpanda rpk topic create bars.book.1s bars.trade.1s predictions -p 1 -r 1 || true
	docker exec redpanda rpk topic alter-config book.l2.raw --set max.message.bytes=10485760

producer:
	.venv/bin/python -m producer

check-gaps:
	.venv/bin/python scripts/check_gaps.py

tables:
	docker exec trino trino --execute "$$(cat scripts/iceberg_tables.sql)"

submit-bars:
	./scripts/submit_bars.sh

# Truncate bars tables before a fresh job submit (it replays from earliest).
# Gold is derived from bars and must be rebuilt with them: replays are not
# guaranteed bar-identical at watermark late-drop edges (idleness is
# wall-clock), and stale gold rows fail the recomputation tests.
reset-bars:
	docker exec trino trino --execute "DELETE FROM iceberg.market.trade_bars_1s; DELETE FROM iceberg.market.book_bars_1s; DELETE FROM iceberg.market.features_5m"

# Incremental gold build: appends feature/label rows for new complete minutes
gold:
	docker exec trino trino --execute "$$(cat scripts/gold_features.sql)"

# Refresh gold, then train LightGBM vs baselines and log to MLflow
train: gold
	.venv/bin/python -m ml.train

serve-features:
	.venv/bin/python -m service.features

# 8010: 8000 is commonly taken by other local apps
serve-api:
	.venv/bin/uvicorn service.api:app --host 0.0.0.0 --port 8010

# Dagster UI on 3001 (Grafana owns 3000)
dagster:
	.venv/bin/dagster dev -f orchestration/definitions.py -p 3001

up:
	docker compose up -d

down:
	docker compose down

# Destroys all data volumes. Use when you want a truly fresh start.
clean:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

smoke:
	./scripts/smoke.sh
