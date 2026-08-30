# Sweden Lever source design

## Scope and source contract

This package collects public Lever postings for reviewed Swedish companies. The
initial EU-hosted board is SEB (`seb`), linked to
`corpscout.se_companies.company_id = '5020329081'`. It pages
`GET https://api.eu.lever.co/v0/postings/{site}?mode=json&skip={n}&limit=100`
until the final short page. Only postings whose official `country` is `SE` are
normalized.

Every page must be a valid JSON list. Any page failure aborts the run before a
manifest is committed, so a partial API response cannot generate closures. A
complete valid empty response is allowed.

## Storage and grains

Raw board responses are content-addressed in `source-sweden-lever`. This source
owns `sweden_lever_source.duckdb` and all eight `se_lever_*` migration-owned
ClickHouse tables. Board, company-link, snapshot, current, version, event,
location, and compensation grains match their names. Salary amount and currency
remain separate fields; no exchange-rate conversion occurs here.

Company IDs come only from reviewed board-company evidence. DuckDB normalization
is set-based. Publishing appends stable versions/events and atomically replaces
only Lever current-state tables.

## Lifecycle and operations

First appearance creates `first_seen`, content changes create `content_changed`,
successful absence creates estimated `closed_by_absence`, and a later appearance
creates `reopened`.
Failed or partial fetches create no lifecycle changes. The daily schedule is
`STOPPED` pending a production canary and repeat-run verification.

## Explicit non-goals

Lever jobs remain independent from Greenhouse, Ashby, SmartRecruiters, and
Platsbanken. This package performs no cross-source matching or deduplication and
does not update existing company hiring views.
