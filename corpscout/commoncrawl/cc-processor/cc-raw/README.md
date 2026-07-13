# cc-raw

Shared Common Crawl raw-data code used by the processor and the legacy downloader.

- `fetch/` owns exact Common Crawl range retrieval and WARC/HTTP record parsing.
- `rawstore/` owns pack, index, manifest, ready-document, checksum, key, and RustFS contracts.
- `rawstate/` owns processing, processed, loaded, and reclaimed marker contracts.

This module has no binary and no orchestration. The WARC-oriented runtime uses `fetch/` directly from
[`cc-enrich-worker`](../cc-enrich-worker/). Legacy downloader-specific chunk planning and execution remain
outside this subsystem in [`cc-download-worker`](../../cc-download-worker/); `rawstore/` and `rawstate/`
retain its persisted contracts.

Both consuming modules depend on `cc-raw` through a local Go module replacement. This keeps application
lifecycles separate without duplicating persisted schemas or WARC parsing behavior.

```bash
cd corpscout/commoncrawl/cc-processor
make -C cc-raw test
make -C cc-raw vet
```

Schema changes are compatibility changes: update `schema_version`, golden JSON fixtures, validation, and
both consuming services together.
