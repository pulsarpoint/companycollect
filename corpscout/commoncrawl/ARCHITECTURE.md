# Common Crawl architecture

The former architecture described the retired URL-part and RustFS raw-pack pipeline and is superseded.

Current WARC catalog, processing, loading, resumability, and deployment behavior is documented in
[`cc-processor/README.md`](cc-processor/README.md).

The authoritative DNS and AXFR scanners are independent subsystems with their own documentation in
[`cc-dns-scan/`](cc-dns-scan/) and [`cc-dns-axfr/`](cc-dns-axfr/).
