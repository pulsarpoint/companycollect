# Common Crawl runtime binaries:
#   cc-enrich-worker -> cc-enrich-worker/bin/cc-enrich-worker
#   cc-crawl         -> cc-crawl/bin/cc-crawl
.PHONY: all build worker crawl vet test clean

all: build                ## default: build all pipeline binaries

build: worker crawl          ## build enrichment worker + orchestrator

worker:                   ## build enrichment worker
	$(MAKE) -C cc-enrich-worker build

crawl:                    ## build the cc-crawl orchestrator
	$(MAKE) -C cc-crawl build

vet:                      ## vet all Go modules
	$(MAKE) -C cc-raw vet
	$(MAKE) -C cc-enrich-worker vet
	$(MAKE) -C cc-crawl vet

test:                     ## test direct fetching, enrichment, and orchestration
	$(MAKE) -C cc-raw test
	$(MAKE) -C cc-enrich-worker test
	$(MAKE) -C cc-crawl test

clean:                    ## remove all build outputs
	$(MAKE) -C cc-enrich-worker clean
	rm -rf cc-crawl/bin
