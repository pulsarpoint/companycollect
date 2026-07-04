# Country Processing Notes

This folder tracks what Corpscout can process for each country and what the
resulting company profile contains. It is separate from `docs/sources/countries`,
which is a broad source-discovery index.

Each country page should answer two groups of questions:

1. Source and ingestion strategy:
   - where the data comes from
   - whether the source supports full bulk, full bulk plus incremental, API, or manual download
   - when the scheduler should run
   - whether downloads are partitioned and what the partition key means

2. Company data coverage:
   - native industry category and any NACE mapping
   - native contact data versus derived contact/domain data
   - translation behavior
   - financial data availability and depth
   - employee-count availability
   - final tables/assets produced by the pipeline

## Countries

| Country | ISO2 | Primary source | Strategy | Profile |
|---|---|---|---|---|
| Brazil | BR | Receita Federal CNPJ bulk snapshots | Monthly full bulk snapshot | [profile](brazil.md), [financial sources](brazil-financial-sources.md), [improvements](brazil-improvements.md), [todo](brazil-todo.md) |
