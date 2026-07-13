# Common Crawl systems

This directory contains independent systems that consume Common Crawl data. The WARC catalog, page
processor, runtime configuration, and deployment tooling now live together under
[`cc-processor/`](cc-processor/). Start with its README for building a catalog, running `cc-crawl`, loading
ClickHouse, or deploying the processing runtime.

## Active subsystems

| Directory | Role |
|---|---|
| [`cc-processor/`](cc-processor/) | Builds the WARC-oriented DuckDB catalog and processes selected pages with `cc-crawl` and `cc-enrich-worker`. |
| [`cc-dns-scan/`](cc-dns-scan/) | Resolves Common Crawl domains against authoritative DNS and stores observations. |
| [`cc-dns-axfr/`](cc-dns-axfr/) | Probes authoritative endpoints for zone-transfer exposure. |
| [`reference-builder/`](reference-builder/) | Builds NACE and page-type reference embeddings used by industry processing. |
| [`embedding-tools/`](embedding-tools/) | Verifies and transforms stored page embeddings. |
| [`load-domain-ranks.sh`](load-domain-ranks.sh) | Loads Common Crawl domain-level webgraph ranks into ClickHouse. |

DNS deployment remains independent under [`deploy/cc_dns_scan/`](deploy/cc_dns_scan/) and
[`deploy/cc_dns_axfr/`](deploy/cc_dns_axfr/). Processor deployment is owned by
[`cc-processor/deploy/`](cc-processor/deploy/).

## Legacy and experimental tools

| Directory | Status |
|---|---|
| [`cc-download-worker/`](cc-download-worker/) | Legacy RustFS raw-pack downloader; the current processor reads Common Crawl directly. |
| [`index-builder/`](index-builder/) | Legacy URL-part worklist builder, superseded by the WARC catalog builder. |
| [`embedding-ab/`](embedding-ab/) | Standalone embedding experiments, not part of the processing runtime. |

Historical designs and implementation plans remain under [`docs/`](docs/). ClickHouse DDL is owned by
[`../clickhouse/migrations/`](../clickhouse/migrations/), never by these applications.
