# Brazil Companies Sources

## Purpose

This document describes the intended source model for
`dagster_v3.defs.brazil_companies`.

`brazil_companies` is the country company-domain package. It should contain
company registry, classification, contact, and company-level risk enrichment
sources. Financial statements stay under `brazil_financial`.

Recommended naming:

```text
dagster_v3.defs.brazil_companies.rfb
dagster_v3.defs.brazil_companies.cnae
dagster_v3.defs.brazil_companies.pgfn

brazil_comp_rfb_*
brazil_comp_cnae_*
brazil_comp_pgfn_*
```

## Source Hierarchy

```text
Brazil companies domain
  RFB - national company registry and establishment data
  CNAE - Brazil activity-code to NACE reference mapping
  PGFN - public active-debt and tax/FGTS debt enrichment
  Future sources
    Receita Federal special status/reference enrichments
    Public sanctions and compliance lists
    Government procurement supplier signals
```

## Source Boundaries

| Source | Domain role | Main identifiers | Storage strategy |
|---|---|---|---|
| RFB | canonical company and establishment registry | CNPJ, CNPJ basico | monthly full snapshot |
| CNAE | activity-code reference mapping | CNAE code, NACE code | small static/reference table |
| PGFN | company debt/risk enrichment | CNPJ, CNPJ basico | quarterly snapshot |

RFB remains the canonical identity source for Brazilian companies. PGFN should
not create companies by itself; it should enrich companies that can be joined to
RFB by CNPJ or CNPJ basico.

## RFB - Company Registry

Source package:

```text
dagster_v3.defs.brazil_companies.rfb
```

What it is:

RFB publishes the public CNPJ company registry bulk dataset. The pipeline uses
monthly full snapshots, not incremental deltas.

What we pull:

| Dataset family | Meaning | Use |
|---|---|---|
| empresas | legal entity/company root data | company identity, legal nature, size, capital |
| estabelecimentos | branch/establishment records | CNPJ, name, address, activity, status |
| socios | partners/shareholders | ownership context where used |
| simples | tax regime flags | Simples/MEI enrichment |
| reference tables | code dictionaries | legal nature, municipality, country, qualification |

Current source-specific design doc:

```text
rfb/docs/brazil_rfb-design.md
```

## CNAE - Activity Classification Reference

Source package:

```text
dagster_v3.defs.brazil_companies.cnae
```

What it is:

CNAE is the Brazilian economic-activity classification. The current package
stores the local mapping from Brazilian CNAE codes to NACE categories.

What we pull:

| Dataset | Meaning | Use |
|---|---|---|
| `br_cnae_to_nace.csv` | CNAE-to-NACE mapping fixture | normalize Brazil activity categories to the shared classification model |

## PGFN - Public Active Debt

Source package:

```text
dagster_v3.defs.brazil_companies.pgfn
```

What it is:

PGFN publishes open files for `Devedores da Uniao`, including active federal
debt and FGTS debt. This is not financial-statement data. It is company-level
risk and debt enrichment.

What we pull:

| Dataset family | Meaning | Use |
|---|---|---|
| nao_previdenciario | general active federal debt | CNPJ debt/risk signal |
| fgts | FGTS debt | labor/tax debt signal |
| previdenciario | social-security debt | tax/social-security debt signal |

Implemented assets:

```text
brazil_comp_pgfn_raw_archives_s3
brazil_comp_pgfn_company_debts_duckdb
brazil_comp_pgfn_company_debts_clickhouse
```

Implemented ClickHouse table:

```text
br_pgfn_company_debts
```

Detailed PGFN design:

```text
docs/brazil_pgfn-design.md
```

## Scheduling

| Source | Suggested cadence | Reason |
|---|---|---|
| RFB | monthly | source publishes full registry snapshots |
| CNAE | manual/on-change | local reference mapping changes rarely |
| PGFN | quarterly | source publishes complete active-debt base quarterly |

PGFN and RFB are both full-snapshot sources. For old partitions, prefer cleanup
assets over retaining every local extracted staging directory forever.
