# Sweden SmartRecruiters source design

## Scope and source contract

This package collects public SmartRecruiters postings for reviewed Swedish
companies. The initial company identifier is H&M Group (`HMGroup`), linked to
the listed group parent H & M Hennes & Mauritz AB (publ),
`corpscout.se_companies.company_id = '5560427220'`. It pages
`GET https://api.smartrecruiters.com/v1/companies/{identifier}/postings` with
`country=se`, then fetches every posting detail endpoint so descriptions and
application URLs are preserved.

List pages must contain `content`, every summary must contain an ID, and every
detail request must succeed. A failure aborts the run before a manifest is
committed, preventing false closures from a partial detail crawl.

## Storage and grains

Raw detail bundles are content-addressed in `source-sweden-smartrecruiters`.
The provider owns one DuckDB file and eight `se_smartrecruiters_*` ClickHouse
tables for boards, reviewed links, snapshots, versions, lifecycle events,
current ads, locations, and compensation facts. Only detail records whose
official location country is `se` are normalized.

The reviewed board-company link is the sole company identity decision. DuckDB
normalization is set-based and ClickHouse current-state replacement is atomic.

## Lifecycle and operations

Complete snapshots generate `first_seen`, `content_changed`, estimated
`closed_by_absence`, and `reopened` events within SmartRecruiters only. A valid
empty snapshot may close old rows;
an HTTP or schema failure may not. The daily schedule remains `STOPPED` until a
production canary confirms rate behavior, all detail fetches, row counts, and
idempotent replay.

## Explicit non-goals

The source does not deduplicate or merge jobs with Platsbanken, Greenhouse,
Lever, or Ashby and does not modify existing company hiring tables or views.
