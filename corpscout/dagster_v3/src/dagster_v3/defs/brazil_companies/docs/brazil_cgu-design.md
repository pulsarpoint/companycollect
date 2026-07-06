# Brazil CGU Sanctions Design

## Source

Portal da Transparencia publishes ZIP CSV downloads for public sanctions and
leniency agreements:

- CEIS: `https://portaldatransparencia.gov.br/download-de-dados/ceis`
- CNEP: `https://portaldatransparencia.gov.br/download-de-dados/cnep`
- CEPIM: `https://portaldatransparencia.gov.br/download-de-dados/cepim`
- Acordos de Leniência:
  `https://portaldatransparencia.gov.br/download-de-dados/acordos-leniencia`

The source pages expose the latest available snapshot date in page JavaScript.
The downloadable ZIP URL is:

```text
https://portaldatransparencia.gov.br/download-de-dados/<slug>/<YYYYMMDD>
```

## Pipeline Shape

The first implementation is raw-first:

1. Discover the current snapshot date for each Portal dataset.
2. Download missing ZIP archives into object storage.
3. Reuse existing archive objects for the same dataset/date.
4. Parse ZIP CSV members into DuckDB raw tables.
5. Export one ClickHouse table per DuckDB table.

Dagster assets are not partitioned in this first pass because the Portal pages
advertise only the current available date per dataset, and those dates can differ
between CEIS, CNEP, CEPIM, and leniency agreements. Object-store keys are still
partitioned by source dataset and source snapshot date.

## Object Keys

```text
brazil_cgu/sanctions/raw_archives/dataset=<dataset>/snapshot_date=<YYYY-MM-DD>/archive.zip
brazil_cgu/sanctions/raw_archives/dataset=<dataset>/snapshot_date=<YYYY-MM-DD>/metadata.json
```

## Tables

DuckDB schema:

```text
brazil_cgu
```

DuckDB tables:

```text
ceis_company_sanctions
cnep_company_sanctions
cepim_blocked_entities
leniency_agreements
leniency_agreement_effects
```

ClickHouse tables:

```text
br_cgu_ceis_company_sanctions
br_cgu_cnep_company_sanctions
br_cgu_cepim_blocked_entities
br_cgu_leniency_agreements
br_cgu_leniency_agreement_effects
```

## Company Filtering

CEIS and CNEP include physical persons and legal entities in the same files. This
company-domain package stores only rows with a 14-digit CNPJ. Skipped rows are
reported as materialization metadata.

CEPIM rows are CNPJ-keyed entity records. Leniency agreements are stored only
when the sanctioned document resolves to a 14-digit CNPJ. Leniency effects do
not contain CNPJ directly and are retained by `agreement_id` for joining to
agreements.

## Later Work

- Validate row counts against real Portal snapshots.
- Decide whether dynamic Dagster partitions are worth adding after schedules are
  defined.
- Add company-level latest/risk views only after raw ClickHouse tables are
  stable.
