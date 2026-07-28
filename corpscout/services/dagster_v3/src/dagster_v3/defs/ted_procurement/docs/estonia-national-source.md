# Estonia national procurement source

Status: **implemented as monthly Dagster assets** alongside the EST/EE TED
partition.

## Official source

The selected national source is the Estonian Public Procurement Register
(`Riigihangete register`, RHR), operated by the State Shared Service Centre.
The official open-data page exposes two monthly XML series:

- procurement and modification notices;
- contract-award and award-modification notices.

This pipeline reads the second series from:

`https://riigihanked.riik.ee/rhr/api/public/v1/opendata/notice_award/YYYY/month/MM/xml`

The Ministry of Finance documentation states that the monthly machine interface
is public and free, uses TED XML, and is available from September 2017. The
current RHR UI identifies the licence as CC BY-SA 3.0 EE. Corpscout starts at
January 2024 to align with the shared eForms-only TED pipeline.

## Asset and storage design

Each monthly partition:

1. downloads the official RHR contract-award XML bundle;
2. queries the TED search API for Estonian buyers over a padded date window;
3. stores both artifacts content-addressed in S3 with an immutable manifest;
4. parses separate notice, lot, and winning-tenderer DuckDB tables;
5. converts EUR winner-grain amounts to USD;
6. atomically replaces that month in ClickHouse and resolves exact eight-digit
   winner identifiers against `ee_companies.reg_code`.

The source tables retain all notice versions. Current views remove a notice
version when a later `ChangedNoticeIdentifier` references it.

## TED overlap

RHR contains both national-only and directive-level awards. The RHR root
`cbc:ID` is the same UUID exposed as TED's `notice-identifier`, so the raw asset
snapshots a UUID-to-publication-number index. `ee_government_contracts` uses all
RHR awards and adds only TED awards whose publication number is not already
present in RHR. This preserves Estonia-buyer awards performed outside Estonia
while avoiding exact RHR/TED duplication.

In the live January 2026 bundle, RHR published 941 notice documents (927 unique
notice UUIDs); 336 UUIDs matched TED exactly.

## Value and company semantics

- Notice totals, notice estimates/framework values, lot values/tender ranges,
  winner tender values, and subcontracting values stay in distinct fields at
  their published grains.
- A tender value shared by a consortium is retained but is not attributed to
  every member. Only a single-winner tender contributes company spend.
- Estonian registry codes are exactly eight digits after whitespace removal.
  Personal codes, malformed identifiers, and VAT identifiers remain raw and
  unmatched; Estonia's VAT number cannot safely be converted into the business
  registry code.
- Foreign winners remain in the national source tables but are not linked to
  the domestic `ee_companies` spine or its company summary.

## Coverage

RHR closes the national and below-threshold gap left by TED for Estonian
contract-award notices. It does not prove that every direct purchase or every
framework call-off was published as an award notice. The pipeline covers awards
with identified tenderers; active opportunities remain available through the
RHR search portal and do not enter company award summaries.
