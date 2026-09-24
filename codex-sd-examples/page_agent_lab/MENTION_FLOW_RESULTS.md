# Mention-first flow: saved-page evaluation

16 September 2026. Direct DeepSeek `deepseek-flash`, high reasoning. Seven existing captures from five companies; no new crawling. Complete results uploaded to the existing RustFS `crawls` bucket and read back for SHA-256 verification.

## Results

Storage follow-up, September 17: all remote experiment objects were deleted at the
user's request, including every revision listed below. Local JSON results remain.
See [the verified deletion receipt](../../corpscout/services/crawler_service/RUSTFS_CLEANUP_RECEIPT.json).
The current v0.18.0 runner returns JSON and no longer uploads it.

| Company | Pages | Raw mentions | Specific technology mentions | Technical context | Other records | Links |
|---|---:|---:|---:|---:|---:|---:|
| https://www.handelsbanken.se/ | 2 | 33 | 22 | 11 | 49 | 31 |
| https://oxide.computer/ | 2 | 28 | 18 | 10 | 21 | 110 |
| https://www.novelic.com/ | 1 | 10 | 1 | 9 | 49 | 100 |
| https://www.rt-rk.com/ | 1 | 8 | 4 | 4 | 26 | 131 |
| https://memgraph.com/ | 1 | 9 | 8 | 1 | 29 | 74 |

Totals: **88 raw mentions: 53 specific-technology mentions and 35 technical-context mentions**, plus **174 nontechnology records and 446 observed links**. Counts are source occurrences, not unique technologies or verified company deployments.

**22 of 23 selected semantic controls passed.** These are regression examples inspected against source, not exhaustive ground truth or a blind model comparison. Capture/hash, mention/decision completeness and upload checks passed. All 88 final model decisions have valid source references; one subsequently received a manual semantic-review flag.

Useful distinctions retained:

- Handelsbanken: Fabric and Databricks plans remain distinct from hiring requirements; Azure Pipelines is team expertise.
- NOVELIC: AURIX development/testing expertise; FPGA, RISC-V, CAD and similar terms remain technical context.
- Oxide: AMD EPYC as a product component; Rancher, OpenShift, Terraform and OpenTofu compatibility; protocols remain technical context.
- RT-RK: Linux, C++ and OBLO relate to the Endress+Hauser SGC200 client project, not inferred internal infrastructure.
- Memgraph: stated Google Meet use; Neo4j and other comparisons remain mentions.

## Failures found and changes tested

1. The initial collection prompt allowed `source_name` to be confused with the job title or page URL. Host source checks held these outputs. Prompt 1.1 adds an explicit field definition and JSON example, and the page request retries a source-name/reference failure at most once. The corrected bank page and five remaining pages were rerun. The valid original team page was reused.
2. Narrow per-mention excerpts omitted the customer relationship for the RT-RK case. Classifier 1.1 adds original company/client/product passages identified through the other page objectives, and sends each repeated section once per batch. Classification was rerun for all five companies without extracting pages again.
3. Six valid decisions cited shared page context, such as the original “Krav:” (Required) list introduction, which was available in the batch but not assigned to that individual mention. Deterministic validation now allows supplied context from the same page while requiring the mention’s own primary section too. This recovered all six without new model calls. Invented, duplicate and cross-page references remain invalid.
4. The classifier inferred an AMD vendor partnership from a component-vendor description. The final Oxide revision preserves that model decision with an explicit `needs_review` flag and explanation. A source-identifiable component vendor does not establish a partnership.

## Important limitations

- All company results remain **partial**. Only **36/174 nontechnology records** pass the inherited quotation/attribution checks. All records and rejections remain present; this is not a claim that the other 138 records are factually false. Those validators and extraction quotations need a separate focused improvement.
- Catalog outcomes across all 88 mentions: **10 matched, 63 ambiguous, 15 not found**. Exact names, case-insensitive matches and accepted aliases resolve; fuzzy candidates are preserved without automatic identity selection. These counts include excluded technical context. A richer identity-resolution pass remains useful.
- The selected controls do not establish complete technology recall. Mention counts include repeated source passages and component aliases.
- The full original source and section metadata make prompts larger. Further reducing redundant prompt metadata and improving section/context selection is an optimization opportunity; no evidence was silently truncated.
- This implements a reusable package page unit, final classifier, saved-page runner and uploader. The existing autonomous URL crawler still uses its earlier controller. Queue integration, full-site coverage testing, interrupted-run resume and downstream ClickHouse ingestion are not implemented by this change.

## Reproducibility

The initial pilot, corrected extraction, final model classification, validation replay and manual review are saved separately under `page_agent_lab/data/mention-flow*20260916`. Every live run freezes source captures and Python code. The first pilot was stopped after the repeated name-field failure; its model calls and completed partial submissions are preserved. `mention_replay.py` reruns only classification, and `mention_validate.py` replays host reference validation without model calls.

Validation: 160 package tests (4 browser tests intentionally skipped), 7 original lab tests, Ruff and type checks. The new tests cover malformed output retry, source-name failures, original client context, native/rendered provenance, invalid reference handling, central lookup statuses and conditional S3 upload/retry/collision behavior. A real identical-payload RustFS retry returned `already_present` and verified the same checksum.

## Final result objects

- https://www.handelsbanken.se/: [complete JSON](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/page_agent_lab/data/mention-flow-validated-20260916/company-5a0868dd06e5/result.json)
  - Object: `s3://crawls/company-research/4204bf5cdb1a438a8451a1b7fc2eb113/0ae6d33d2c494f2b83a56e0a22e939e9/result.json`
  - SHA-256: `208a7c94fe3dbba74acf399b1777ba6f511abcc86faffd7467d406ba17ff96ae`
- https://oxide.computer/: [complete JSON](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/page_agent_lab/data/mention-flow-review-20260916/oxide/result.json)
  - Object: `s3://crawls/company-research/7225362ee5a54a0f9131ef3761cb1fc7/7ff6030d630448c3a50cf41eba0d6a9e/result.json`
  - SHA-256: `efc75d8c436de72e0cc5dbaefa60b4a2b2303577661f9852a8eb5ccc92deaf1c`
- https://www.novelic.com/: [complete JSON](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/page_agent_lab/data/mention-flow-validated-20260916/company-b1dbd6b65ef6/result.json)
  - Object: `s3://crawls/company-research/e8b312d7d2c349e4bfc44cc028c1387d/2f0cd5297a974baf997fba3767440a2a/result.json`
  - SHA-256: `fcccbfda24ec4cf2122c29ce51322409d238a5e03c75114a3e4952463bf239f9`
- https://www.rt-rk.com/: [complete JSON](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/page_agent_lab/data/mention-flow-validated-20260916/company-c1bb70f013fd/result.json)
  - Object: `s3://crawls/company-research/7c9eb8fc7d7142f49912c4d9ea731a39/83bb152117c442b8a8ed25f22a8040e9/result.json`
  - SHA-256: `20a338ae542cc7ef1ba95a4d84f499baaf37c7e05222585de5aaf322797eeb29`
- https://memgraph.com/: [complete JSON](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/page_agent_lab/data/mention-flow-validated-20260916/company-ffdca72ef7c5/result.json)
  - Object: `s3://crawls/company-research/085fd329b2a84656b986667c343e165a/21cc563586a24ca399cd8628c4dd932d/result.json`
  - SHA-256: `224601f7a6c98ebdd55857d57c4eb60d4826ac0ef97cdc1f8089a28d345dc528`
