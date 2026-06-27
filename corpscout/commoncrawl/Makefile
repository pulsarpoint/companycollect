# CommonCrawl enrichment — build both Go binaries with a single `make`:
#   cc-enrich-worker  (the enrichment pass + loader)   -> cc-enrich-worker/bin/cc-enrich-worker
#   cc-crawl          (the orchestrator, ex run_crawl.sh) -> cc-crawl/bin/cc-crawl
.PHONY: all build worker crawl vet test clean

all: build                ## default: build both binaries

build: worker crawl       ## build cc-enrich-worker + cc-crawl

worker:                   ## build the enrichment worker
	$(MAKE) -C cc-enrich-worker build

crawl:                    ## build the cc-crawl orchestrator
	$(MAKE) -C cc-crawl build

vet:                      ## go vet both
	$(MAKE) -C cc-enrich-worker vet
	$(MAKE) -C cc-crawl vet

test:                     ## test the worker
	$(MAKE) -C cc-enrich-worker test

clean:                    ## remove both bin/ dirs
	$(MAKE) -C cc-enrich-worker clean
	rm -rf cc-crawl/bin
