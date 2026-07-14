TARGET_GOOS ?= linux
TARGET_GOARCH ?= amd64
DIST_DIR ?= $(CURDIR)/dist/$(TARGET_GOOS)-$(TARGET_GOARCH)
ENV_FILE ?= .env
PAGES_PER_DOMAIN ?= 25

.PHONY: all build builder worker catalog test builder-test vet verify runtime-test runtime-vet runtime-verify release clean

all: build

build: builder worker

builder:
	$(MAKE) -C cc-warc-index-builder build

worker:
	$(MAKE) -C cc-enrich-worker build

catalog:
	@test -n "$(CRAWL)" || { echo "CRAWL is required, for example CRAWL=CC-MAIN-2026-25" >&2; exit 2; }
	@test -f "$(ENV_FILE)" || { echo "processor environment file not found: $(ENV_FILE)" >&2; exit 2; }
	cd cc-warc-index-builder && uv run --frozen --env-file "$(abspath $(ENV_FILE))" \
		cc-warc-index-builder \
		--crawl "$(CRAWL)" \
		--pages-per-domain "$(PAGES_PER_DOMAIN)" \
		$(CATALOG_ARGS)

builder-test:
	$(MAKE) -C cc-warc-index-builder test

runtime-test:
	$(MAKE) -C cc-enrich-worker test

test: builder-test runtime-test

runtime-vet:
	$(MAKE) -C cc-enrich-worker vet

vet: runtime-vet

runtime-verify: runtime-test runtime-vet

verify: test vet

release: runtime-verify
	rm -rf "$(DIST_DIR)"
	mkdir -p "$(DIST_DIR)"
	docker build \
		--platform "$(TARGET_GOOS)/$(TARGET_GOARCH)" \
		--file deploy/runtime.Dockerfile \
		--output "type=local,dest=$(DIST_DIR)" \
		.
	test -x "$(DIST_DIR)/cc-enrich-worker"

clean:
	$(MAKE) -C cc-warc-index-builder clean
	$(MAKE) -C cc-enrich-worker clean
	rm -rf dist
