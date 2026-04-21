COMPOSE ?= docker compose
SCRAPER_IMAGE ?= sweb-scoreboard-monitor-scraper-test

.PHONY: up down restart ps logs logs-scraper logs-grafana logs-influx test build

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) up -d --build --force-recreate

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

logs-scraper:
	$(COMPOSE) logs -f --tail=200 scraper

logs-grafana:
	$(COMPOSE) logs -f --tail=200 grafana

logs-influx:
	$(COMPOSE) logs -f --tail=200 influxdb

build:
	docker build -t $(SCRAPER_IMAGE) scraper

test: build
	docker run --rm $(SCRAPER_IMAGE) python -m pytest -q /app/tests
