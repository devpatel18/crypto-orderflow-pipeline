.PHONY: up down clean ps logs smoke

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
