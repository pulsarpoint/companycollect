# Common Crawl contains two independently deployed systems:
#   cc-processor -> catalog builder plus WARC processing runtime
#   DNS          -> authoritative scanner and AXFR scanner

.PHONY: all build processor builder worker crawl dns-scan dns-axfr vet test verify runtime-vet runtime-test runtime-verify release clean

all: build

build: processor dns-scan dns-axfr

processor:
	$(MAKE) -C cc-processor build

builder:
	$(MAKE) -C cc-processor builder

worker:
	$(MAKE) -C cc-processor worker

crawl:
	$(MAKE) -C cc-processor crawl

dns-scan:
	$(MAKE) -C cc-dns-scan build

dns-axfr:
	$(MAKE) -C cc-dns-axfr build

test:
	$(MAKE) -C cc-processor test
	$(MAKE) -C cc-dns-scan test
	$(MAKE) -C cc-dns-axfr test

vet:
	$(MAKE) -C cc-processor vet
	$(MAKE) -C cc-dns-scan vet
	$(MAKE) -C cc-dns-axfr vet

verify: test vet

runtime-test:
	$(MAKE) -C cc-processor runtime-test

runtime-vet:
	$(MAKE) -C cc-processor runtime-vet

runtime-verify:
	$(MAKE) -C cc-processor runtime-verify

release:
	$(MAKE) -C cc-processor release

clean:
	$(MAKE) -C cc-processor clean
	$(MAKE) -C cc-dns-scan clean
	$(MAKE) -C cc-dns-axfr clean
