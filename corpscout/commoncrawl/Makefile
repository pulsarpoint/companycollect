# Common Crawl runtime binaries:
#   cc-enrich-worker -> cc-enrich-worker/bin/cc-enrich-worker
#   cc-crawl         -> cc-crawl/bin/cc-crawl
#   cc-dns-scan      -> cc-dns-scan/bin/cc-dns-scan
#   cc-dns-axfr      -> cc-dns-axfr/bin/cc-dns-axfr
TARGET_GOOS ?= linux
TARGET_GOARCH ?= amd64
DIST_DIR ?= $(CURDIR)/dist/$(TARGET_GOOS)-$(TARGET_GOARCH)

.PHONY: all build worker crawl dns-scan dns-axfr vet test verify runtime-vet runtime-test runtime-verify release clean

all: build                    ## default: build all Common Crawl binaries

build: worker crawl dns-scan dns-axfr

worker:                   ## build enrichment worker
	$(MAKE) -C cc-enrich-worker build

crawl:                    ## build the cc-crawl orchestrator
	$(MAKE) -C cc-crawl build

dns-scan:                 ## build the authoritative DNS scanner
	$(MAKE) -C cc-dns-scan build

dns-axfr:                 ## build the AXFR scanner
	$(MAKE) -C cc-dns-axfr build

vet:                      ## vet all Go modules
	$(MAKE) -C cc-raw vet
	$(MAKE) -C cc-enrich-worker vet
	$(MAKE) -C cc-crawl vet
	$(MAKE) -C cc-dns-scan vet
	$(MAKE) -C cc-dns-axfr vet

test:                     ## test every Common Crawl Go module
	$(MAKE) -C cc-raw test
	$(MAKE) -C cc-enrich-worker test
	$(MAKE) -C cc-crawl test
	$(MAKE) -C cc-dns-scan test
	$(MAKE) -C cc-dns-axfr test

verify: test vet          ## test and vet every runtime module

runtime-test:             ## test the paired Common Crawl processing runtime
	$(MAKE) -C cc-raw test
	$(MAKE) -C cc-enrich-worker test
	$(MAKE) -C cc-crawl test

runtime-vet:              ## vet the paired Common Crawl processing runtime
	$(MAKE) -C cc-raw vet
	$(MAKE) -C cc-enrich-worker vet
	$(MAKE) -C cc-crawl vet

runtime-verify: runtime-test runtime-vet

release: runtime-verify  ## build the paired Linux runtime locally with its CGO dependency
	rm -rf $(DIST_DIR)
	mkdir -p $(DIST_DIR)
	docker build \
		--platform $(TARGET_GOOS)/$(TARGET_GOARCH) \
		--file deploy/runtime.Dockerfile \
		--output type=local,dest=$(DIST_DIR) \
		.
	test -x $(DIST_DIR)/cc-enrich-worker
	test -x $(DIST_DIR)/cc-crawl

clean:                    ## remove all build outputs
	$(MAKE) -C cc-enrich-worker clean
	$(MAKE) -C cc-dns-scan clean
	$(MAKE) -C cc-dns-axfr clean
	rm -rf cc-crawl/bin
	rm -rf dist
