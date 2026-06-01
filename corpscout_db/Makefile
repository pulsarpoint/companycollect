SHELL := /bin/bash

PSQL ?= psql
ENV_FILE ?= .env

ifneq ("$(wildcard $(ENV_FILE))","")
include $(ENV_FILE)
export
endif

POSTGRES_HOST ?= companycollect
POSTGRES_PORT ?= 5432
POSTGRES_ADMIN_DB ?= postgres
POSTGRES_ADMIN_USER ?= corpscout

CORPSCOUT_DB ?= corpscout
CORPSCOUT_USER ?= corpscout
CORPSCOUT_ANON_ROLE ?= corpscout_anon
TEMPORAL_DB ?= temporal
TEMPORAL_VISIBILITY_DB ?= temporal_visibility
TEMPORAL_USER ?= temporal

ADMIN_DSN := host=$(POSTGRES_HOST) port=$(POSTGRES_PORT) dbname=$(POSTGRES_ADMIN_DB) user=$(POSTGRES_ADMIN_USER) sslmode=disable
CORPSCOUT_DSN := host=$(POSTGRES_HOST) port=$(POSTGRES_PORT) dbname=$(CORPSCOUT_DB) user=$(POSTGRES_ADMIN_USER) sslmode=disable

.PHONY: up down bootstrap bootstrap-cluster verify psql

up:
	docker compose --env-file $(ENV_FILE) up -d postgres

down:
	docker compose --env-file $(ENV_FILE) down

bootstrap: bootstrap-cluster verify

bootstrap-cluster:
	PGPASSWORD="$${POSTGRES_ADMIN_PASSWORD}" $(PSQL) "$(ADMIN_DSN)" \
		-v ON_ERROR_STOP=1 \
		-v corpscout_db="$(CORPSCOUT_DB)" \
		-v corpscout_user="$(CORPSCOUT_USER)" \
		-v corpscout_password="$${CORPSCOUT_PASSWORD}" \
		-v temporal_db="$(TEMPORAL_DB)" \
		-v temporal_visibility_db="$(TEMPORAL_VISIBILITY_DB)" \
		-v temporal_user="$(TEMPORAL_USER)" \
		-v temporal_password="$${TEMPORAL_PASSWORD}" \
		-f bootstrap/001_roles.sql \
		-f bootstrap/002_databases.sql \
		-f bootstrap/003_database_grants.sql

verify:
	PGPASSWORD="$${POSTGRES_ADMIN_PASSWORD}" $(PSQL) "$(ADMIN_DSN)" \
		-v ON_ERROR_STOP=1 \
		-v corpscout_db="$(CORPSCOUT_DB)" \
		-v temporal_db="$(TEMPORAL_DB)" \
		-v temporal_visibility_db="$(TEMPORAL_VISIBILITY_DB)" \
		-v corpscout_user="$(CORPSCOUT_USER)" \
		-v corpscout_anon_role="$(CORPSCOUT_ANON_ROLE)" \
		-v temporal_user="$(TEMPORAL_USER)" \
		-f verify.sql

psql:
	PGPASSWORD="$${POSTGRES_ADMIN_PASSWORD}" $(PSQL) "$(ADMIN_DSN)"
