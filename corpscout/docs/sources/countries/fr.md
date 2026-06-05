# France (FR) - GDP Rank #7

> **Summary:** Corpscout uses the official SIRENE bulk parquet files published on data.gouv.fr as the base France model. The first supported ingest downloads two current stock files: `StockUniteLegale` for legal units keyed by SIREN and `StockEtablissement` for establishments keyed by SIRET.

## Official Registry

### INSEE SIRENE Bulk Parquet Files
| Field      | Detail |
|------------|--------|
| URL        | https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/ |
| Access     | Bulk parquet download through stable data.gouv resource URLs |
| Cost       | Free |
| Auth       | None |
| Rate limit | Not documented for bulk files; avoid repeated downloads by reusing staged files where possible. |
| Notes      | SIREN identifies legal units. SIRET identifies establishments and links back to SIREN. The data.gouv resource API URLs are stable and redirect/resolve to the current static monthly parquet file. |

### Corpscout Base Files
| Dataset key | File | Stable resource URL | Approx size observed 2026-06-05 |
|-------------|------|---------------------|----------------------------------|
| `stock_unite_legale` | `StockUniteLegale` | https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3 | 695 MB |
| `stock_etablissement` | `StockEtablissement` | https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f | 2.18 GB |

The base ingest downloads about 2.9 GB for one full run. These files are enough for the current company registry model: legal identity from `StockUniteLegale`, and establishments, addresses, and headquarters flags from `StockEtablissement`.

### SIRENE API
| Field      | Detail |
|------------|--------|
| URL        | https://api.insee.fr/entreprises/sirene/V3/ |
| Access     | API |
| Cost       | Free |
| Fields     | SIREN, SIRET, legal unit fields, establishment fields, addresses, activity codes, legal category, creation dates, status |
| Auth       | Registration/API access may be required depending on endpoint and platform. |
| Rate limit | Per INSEE/API platform policy |
| Notes      | Useful for lookup or incremental enrichment, but Corpscout's France base ingest uses bulk parquet files so we do not need pre-existing SIREN/SIRET identifiers. |

### Infogreffe
| Field      | Detail |
|------------|--------|
| URL        | https://www.infogreffe.fr |
| Access     | API (paid) / Paid document retrieval |
| Cost       | Paid — per-document fees (EUR 3–15); API via subscription |
| Fields     | Kbis extract (certified company status), RCS number, directors, shareholders, capital, activity, filing history |
| Auth       | Registration required; API key for programmatic access |
| Rate limit | Per plan |
| Notes      | Infogreffe is operated by the network of commercial court registries (greffes). Kbis is the official French company certificate. Required for many legal/compliance processes. Provides real-time certified data. |

### BODACC (Bulletin Officiel des Annonces Civiles et Commerciales)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bodacc.fr / https://api.piste.gouv.fr/dila/bodacc/v1/annonces |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Company name, SIREN, registration, liquidation, procedure collective (insolvency), sale of business announcements |
| Auth       | Free API key via PISTE platform |
| Rate limit | None documented |
| Notes      | Official legal gazette for commercial announcements. Excellent for insolvency and status change monitoring. Data goes back to 1985. |

## Commercial Providers

### Société.com / Ellisphere
| Field      | Detail |
|------------|--------|
| URL        | https://www.societe.com / https://www.ellisphere.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | SIREN, financials (annual accounts), executives, shareholders, credit score, sectoral analysis |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Société.com is a popular web aggregator; Ellisphere is the professional B2B data arm. Financial statement data (bilans) is a key differentiator. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from SIRENE; no advantage over the free direct SIRENE API |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed French companies only; CAC 40 and SBF 120 well covered |

## Corpscout Status
- [x] Base raw ingest workflow implemented
- [x] Source-profile normalization workflow implemented
- Source name: `france`
- Workflow schema: `france_workflow`
- Source schema: `france_source`
- Recommended source: SIRENE bulk parquet files from data.gouv.fr
- Priority: High

## Corpscout Processing

### Download and staging
The UI action starts the Temporal workflow `LoadFranceBulkRawRecords` on task queue `france-bulk-ingest`.

The first activity, `StageFranceBulkRawFilesActivity`, downloads both base parquet files before any parsing starts:

1. `StockUniteLegale` from `https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3`.
2. `StockEtablissement` from `https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f`.

Inside the scheduler container, files are staged under:

```text
/var/lib/corpscout/worksets/france-sirene/<temporal-workflow-id>/
```

Expected staged filenames:

```text
stock_unite_legale.parquet
stock_etablissement.parquet
```

In Docker Compose this is backed by the existing scheduler workset mount:

```text
./data/scheduler-worksets:/var/lib/corpscout/worksets
```

So on the local host the France files are visible under:

```text
./data/scheduler-worksets/france-sirene/<temporal-workflow-id>/
```

The staging path is intentionally an internal scheduler path, not an environment setting. Staged parquet files are kept after ingestion for inspection and reprocessing; cleanup should be a separate manual or maintenance action.

### Database audit rows
During staging, Corpscout creates:

- `france_workflow.workflow_runs` row for the Temporal run.
- `france_workflow.bulk_snapshots` row for the bulk snapshot.
- `france_workflow.source_files` rows for both downloaded parquet files.

Each source-file row records dataset key, resource ID, stable URL, resolved URL, filename, content type, content length, SHA-256 checksum, status, and metadata. Metadata includes the local staged path.

### Parallel parsing
After both files are staged, the parent workflow starts two child workflows in parallel:

| Child workflow | Input file | Target raw table |
|----------------|------------|------------------|
| `ProcessFranceSireneStockUniteLegale` | `stock_unite_legale.parquet` | `france_workflow.raw_legal_units` |
| `ProcessFranceSireneStockEtablissement` | `stock_etablissement.parquet` | `france_workflow.raw_establishments` |

Both child workflows stream the local parquet file from disk and upsert rows in database batches. The file download always happens for the full remote file. The UI record limit only limits how many parquet rows are parsed from each already-downloaded file.

When both child workflows finish, `FinishFranceBulkRawIngestActivity` marks the snapshot parsed and finishes the workflow run with aggregate row counts. If staging or parsing fails, the workflow marks the run failed by Temporal workflow ID.

### Source profile normalization
After raw rows exist in `france_workflow`, the raw input UI exposes `Build source profile`.

That action starts Temporal workflow `NormalizeFranceSourceProfiles` on task queue `france-source-profile`. The workflow processes current raw legal units in chunks. For each selected legal-unit chunk it:

1. Upserts active company rows in `france_source.companies` from `france_workflow.raw_legal_units`.
2. Upserts active establishment rows in `france_source.establishments` from current `france_workflow.raw_establishments` with matching SIREN values.
3. Upserts headquarters, establishment, and secondary addresses in `france_source.addresses`.
4. Rebuilds SIRENE activity-code rows in `france_source.industries` for the selected active companies and establishments.

The workflow skips raw legal units that already have an active source company with the same payload hash and no changed/missing current establishments, unless explicit IDs are passed. SIRENE base files do not provide websites, domains, or contacts, so those tables stay empty until separate enrichment workflows populate them.

### Current scope
The base France model intentionally uses only:

- `StockUniteLegale`
- `StockEtablissement`

The following SIRENE parquet files are not part of the current base ingest:

- `StockUniteLegaleHistorique`
- `StockEtablissementHistorique`
- `StockEtablissementLiensSuccession`
- `StockDoublons`
