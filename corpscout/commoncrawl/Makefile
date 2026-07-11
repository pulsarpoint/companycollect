# Common Crawl pipeline binaries:
#   cc-enrich-worker   -> cc-enrich-worker/bin/cc-enrich-worker
#   cc-download-worker -> cc-download-worker/bin/cc-download-worker
#   cc-warc-analyzer   -> cc-download-worker/bin/cc-warc-analyzer
#   cc-crawl           -> cc-crawl/bin/cc-crawl
.PHONY: all build worker download crawl vet test clean

all: build                ## default: build all pipeline binaries

build: worker download crawl ## build downloader + enrichment worker + orchestrator

worker:                   ## build enrichment worker
	$(MAKE) -C cc-enrich-worker build

download:                 ## build only the raw download worker
	$(MAKE) -C cc-download-worker build

crawl:                    ## build the cc-crawl orchestrator
	$(MAKE) -C cc-crawl build

vet:                      ## vet all Go modules
	$(MAKE) -C cc-raw vet
	$(MAKE) -C cc-download-worker vet
	$(MAKE) -C cc-enrich-worker vet
	$(MAKE) -C cc-crawl vet

test:                     ## test shared contracts, both workers, and the orchestrator
	$(MAKE) -C cc-raw test
	$(MAKE) -C cc-download-worker test
	$(MAKE) -C cc-enrich-worker test
	$(MAKE) -C cc-crawl test

clean:                    ## remove all build outputs
	$(MAKE) -C cc-download-worker clean
	$(MAKE) -C cc-enrich-worker clean
	rm -rf cc-crawl/bin
