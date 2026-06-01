SHELL := /bin/bash

SERVICES_ENV ?= $(if $(wildcard deploy/services/.env),deploy/services/.env,deploy/services/.env.example)
SERVICES_COMPOSE := docker compose --env-file $(SERVICES_ENV) -f deploy/services/docker-compose.yml

.PHONY: services-up services-down services-pull services-logs services-restart
.PHONY: corpscout-up corpscout-down corpscout-logs db-up db-bootstrap db-verify
.PHONY: test-corpscout-scheduler test-translation-service test-crawl-service

services-up:
	$(SERVICES_COMPOSE) up -d

services-down:
	$(SERVICES_COMPOSE) down

services-pull:
	$(SERVICES_COMPOSE) pull

services-logs:
	$(SERVICES_COMPOSE) logs -f

services-restart: services-pull
	$(SERVICES_COMPOSE) up -d

corpscout-up:
	$(MAKE) -C corpscout up

corpscout-down:
	$(MAKE) -C corpscout down

corpscout-logs:
	$(MAKE) -C corpscout logs

db-up:
	$(MAKE) -C corpscout_db up

db-bootstrap:
	$(MAKE) -C corpscout_db bootstrap

db-verify:
	$(MAKE) -C corpscout_db verify

test-corpscout-scheduler:
	cd corpscout/scheduler && GOWORK=off go test ./...

test-translation-service:
	cd data-pipelines/services/translation-service && uv run pytest -q

test-crawl-service:
	cd data-pipelines/services/crawl-service && uv run pytest -q
