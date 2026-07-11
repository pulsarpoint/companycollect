# Common Crawl pipeline binaries:
#   cc-enrich-worker   -> cc-enrich-worker/bin/cc-enrich-worker
#   cc-download-worker -> cc-enrich-worker/bin/cc-download-worker
#   cc-crawl           -> cc-crawl/bin/cc-crawl
.PHONY: all build worker download crawl vet test clean

all: build                ## default: build all pipeline binaries

build: worker crawl       ## build downloader + enrichment worker + orchestrator

worker:                   ## build download and enrichment workers
	$(MAKE) -C cc-enrich-worker build

download:                 ## build only the raw download worker
	$(MAKE) -C cc-enrich-worker download

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
