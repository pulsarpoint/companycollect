# cc-raw

Shared Common Crawl raw-data contracts used by the sibling download and enrichment services.

- `fetch/` owns exact Common Crawl range retrieval and WARC/HTTP record parsing.
- `rawstore/` owns pack, index, manifest, ready-document, checksum, key, and RustFS contracts.
- `rawstate/` owns processing, processed, loaded, and reclaimed marker contracts.

This module has no binary and no orchestration. Downloader-specific chunk planning and execution remain in
`../cc-download-worker`; enrichment workflows remain in `../cc-enrich-worker`.

Both service modules depend on `cc-raw` through a local Go module replacement. This keeps application
lifecycles separate without duplicating persisted schemas or WARC parsing behavior.

```bash
make test
make vet
```

Schema changes are compatibility changes: update `schema_version`, golden JSON fixtures, validation, and
both consuming services together.
