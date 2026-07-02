# cc-enrich-worker

The **processor** of the CommonCrawl enrichment pipeline: a single Go binary that byte-range-fetches
WARC pages from S3, runs one of two workflows over them (industry classification or technology/company-info
extraction), writes **Parquet**, and — as a separate step — **loads** that Parquet into ClickHouse.

It is a subcommand CLI: `cc-enrich-worker <industry|embed|tech|both|load> [flags]`. The command **is** the
workflow. It does not orchestrate parts or build worklists — that's `cc-crawl` and `index-builder`
(see [`../README.md`](../README.md)).

**Two stages, and every ClickHouse `INSERT` is in stage 2 (`load`):**

| Stage | Command | Does | ClickHouse |
|---|---|---|---|
| **produce** | `industry` / `embed` / `tech` / `both` | fetch → (embed/classify \| detect tech) → write Parquet | only **reads** the NACE reference (industry/both) |
| **load** | `load` | read the produced Parquet → `INSERT` into `commoncrawl_*` | the **only** writer |

Splitting them means a produce can be inspected before it's loaded, a load can re-run without re-fetching,
and a crashed produce never half-writes ClickHouse. Both stages use the native ClickHouse protocol, so
`clickhouse-client` is never required.

> Full pipeline context (cc-crawl driver, workflows A/B, per-part loop, the ClickHouse schema):
> [`../README.md`](../README.md) and [`../docs/schema.md`](../docs/schema.md). This doc is the worker's
> CLI + internals reference.
