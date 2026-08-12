.PHONY: migrate-up migrate-up-one migrate-down migrate-version migrate-force migration-create

BACKOFFICE_POSTGRES_MIGRATE_URL := $(or $(BACKOFFICE_POSTGRES_MIGRATE_URL),$(shell sed -n 's/^BACKOFFICE_POSTGRES_MIGRATE_URL=//p' .env 2>/dev/null | tail -n 1))
MIGRATE_IMAGE ?= migrate/migrate:v4.17.0
MIGRATIONS_DIR := $(CURDIR)/database/migrations

_MIGRATE_RUN = docker run --rm -v "$(MIGRATIONS_DIR):/migrations:ro" $(MIGRATE_IMAGE) -path=/migrations -database="$(BACKOFFICE_POSTGRES_MIGRATE_URL)"

define require_migration_url
	@test -n "$(BACKOFFICE_POSTGRES_MIGRATE_URL)" || (echo "BACKOFFICE_POSTGRES_MIGRATE_URL is required (set it in .env or the environment)." >&2; exit 1)
endef

migrate-up:
	$(require_migration_url)
	@$(_MIGRATE_RUN) up

migrate-up-one:
	$(require_migration_url)
	@$(_MIGRATE_RUN) up 1

migrate-down:
	$(require_migration_url)
	@$(_MIGRATE_RUN) down 1

migrate-version:
	$(require_migration_url)
	@$(_MIGRATE_RUN) version

migrate-force:
	$(require_migration_url)
	@test -n "$(VERSION)" || (echo "VERSION is required, for example: make migrate-force VERSION=1" >&2; exit 1)
	@$(_MIGRATE_RUN) force "$(VERSION)"

migration-create:
	@test -n "$(NAME)" || (echo "NAME is required, for example: make migration-create NAME=add_review_assignments" >&2; exit 1)
	@docker run --rm -v "$(MIGRATIONS_DIR):/migrations" $(MIGRATE_IMAGE) \
		create -ext sql -dir /migrations -seq -digits 6 "$(NAME)"
