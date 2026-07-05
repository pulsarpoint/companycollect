# Brazil PGFN Design

## Source

PGFN publishes the `Devedores da Uniao` open dataset for `Divida Ativa da Uniao`
and FGTS. The source page says the complete active-debt base is published
quarterly, the files are open `.CSV`, and the ZIPs are segmented by debt-origin
system and state.

Source page:
`https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/transparencia-fiscal-1/dados-abertos`

Field dictionary:
`https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/transparencia-fiscal-1/arquivos-dados-abertos/dicionario_de_campos.xlsx`

## Dagster Shape

- Package: `dagster_v3.defs.brazil_companies.pgfn`
- Domain docs folder: `dagster_v3.defs.brazil_companies.docs`
- Group: `brazil_comp_pgfn`
- Raw bucket: `source-brazil-pgfn`
- DuckDB path: `data/brazil_pgfn_source.duckdb`
- DuckDB schema: `brazil_pgfn`
- ClickHouse table: `corpscout.br_pgfn_company_debts`

Assets:

- `brazil_comp_pgfn_raw_archives_s3`: downloads quarterly PGFN ZIP archives.
- `brazil_comp_pgfn_company_debts_duckdb`: parses ZIP CSV members into
  normalized CNPJ/company debt rows.
- `brazil_comp_pgfn_company_debts_clickhouse`: publishes the normalized stage to
  ClickHouse.

## Grain

The normalized table has one row per company debt inscription in a quarterly
snapshot. CPF/person rows are ignored because this package is a company
enrichment source.

This keeps the source detail needed for later views:

- current public debt flag per CNPJ;
- total consolidated debt by CNPJ;
- debt counts by situation and source system;
- judicial-collection flag;
- trend across quarterly snapshots.

## Source Systems

Each quarter normally has three ZIPs:

- `nao_previdenciario`: `Divida Ativa Geral`, Sistema SIDA.
- `fgts`: `Divida FGTS`.
- `previdenciario`: `Divida Previdenciaria`, Sistema Divida.

The official 2022 Q4 non-previdenciary ZIP uses the filename
`Dados_abertos_Nao_Previdenciariozip.zip`; this is encoded explicitly.

## Relationship To RFB

PGFN is not a company registry. It should be joined to RFB records by CNPJ or
CNPJ basico:

- use full CNPJ for establishment-level debt details;
- use CNPJ basico for company-root debt rollups;
- keep RFB as the canonical source for company name, address, status, and
  activity fields.

## Scheduling

PGFN describes the publication cadence as quarterly. Materialize the latest
quarter after PGFN publishes it; older quarters are historical backfill.

PGFN archives are full snapshots, not deltas. Reprocessing a quarter should
replace that quarter's DuckDB rows and ClickHouse rows.
