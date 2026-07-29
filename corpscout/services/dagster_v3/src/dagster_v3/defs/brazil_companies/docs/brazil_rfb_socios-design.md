# Brazil RFB Socios design doc — company connections

**Status:** designed 2026-07-28, not built.

**What this adds:** the edge that says a company is connected to something else —
another company, a named person, or a foreign entity. Nothing in the schema holds
this today, for any country.

## 0. Why this exists

`brazil_rfb-design.md` deferred `Socios` three times, always for the same reason:
it carries natural-person names and masked CPFs under LGPD, and phase 1 did not
define the privacy controls. That deferral was correct then. §6 below is the
design it asked for.

What was deferred with it is larger than a people list. `Socios` is the only
Brazilian source that answers **who controls a company and what else they
control** — corporate ownership links for all 68.6M companies, where CVM's
shareholder data covers 1,230. It is also the only route to "these two companies
share an administrator", which links companies with no ownership relation
between them at all.

The decision that shapes the model (user, 2026-07-28): **an edge is an edge**,
whether the other end is a person or a company. One relation table, one
discriminator, no separate people and ownership models.

## 1. Source overview

```
publisher     Receita Federal do Brasil (RFB), CNPJ open data
file          Socios*.zip / SOCIOCSV.zip, in the SAME monthly ZIP set as
              Empresas and Estabelecimentos -- already discovered, not yet fetched
format        headerless CSV, Latin-1, fixed published column order, ';' delimited
volume        ~20-25M partner rows per monthly snapshot
key           cnpj_basico (legal entity), joins br_companies
dictionary    the layout below is written from knowledge of the RFB format and
              is NOT yet verified -- see §10, gate 1
```

The 11 published columns, in order:

```
cnpj_basico · identificador_socio · nome_socio_razao_social · cnpj_cpf_socio
qualificacao_socio · data_entrada_sociedade · pais
representante_legal · nome_representante · qualificacao_representante · faixa_etaria
```

`identificador_socio` is the discriminator that makes one edge model work:
**1 = company · 2 = natural person · 3 = foreign**.

## 2. Ingest mode — and why

**A tenth family inside the existing `brazil_companies/rfb` module**, not a
separate module, though `brazil_rfb-design.md` asked for separation.

The decisive reason is **snapshot consistency**. Partner rows reference
`cnpj_basico` values that must exist in the *same month's* `Empresas`. The RFB
module is `MonthlyPartitionsDefinition`, so inside a partition that holds by
construction. A separate module would re-implement snapshot discovery and drift
the first time a month is re-run, producing dangling edges that read as data
quality problems but are coordination bugs.

The separation the doc wanted is still delivered, in the two places it is
actually enforceable:

- **Storage**: its own `socios.duckdb` stage file and its own concurrency pool,
  matching how every other family is already isolated. A Socios failure cannot
  block or corrupt the register chain.
- **Serving**: its own ClickHouse table with its own access rules (§6).

A module wall would have given the *appearance* of a boundary while costing
snapshot integrity. The boundary that matters is around who can read the names,
not around which Python package parses the CSV.

## 3. Loading

`DEFAULT_FAMILIES` derives from `RAW_TABLE_BY_FAMILY`, so one entry each in that
dict and in `_FAMILY_PATTERNS` makes the downloader fetch the archive. The
existing missing-families guard then *requires* it — a month where RFB does not
publish Socios fails loudly instead of silently yielding an empty graph.

`brazil_comp_rfb_socios_duckdb`: `deps=[SNAPSHOT_FILES]`, own pool, own stage
path, `BackfillPolicy.multi_run(max_partitions_per_run=1)` like its siblings.
Extracts the ZIP member and reads it with DuckDB's native CSV reader
(`all_varchar=true`, positional column tuple). **Never row-by-row Python** — at
20-25M rows that is the slow path the guidelines forbid.

## 4. Transform

Verbatim. Trim, parse `data_entrada_sociedade` to `Date32`, coalesce every
String to `''`. No joins, no resolution, no bucketing, no vocabulary mapping.

**`related_tax_id` is deliberately not resolved to a company at ingest.** When
`related_entity_kind = '1'` it is a full 14-digit CNPJ, while
`br_companies.cnpj_basico` is only the 8-digit CNPJ root (`cnpj_basico` +
`cnpj_ordem` + `cnpj_dv` makes the 14-digit CNPJ, see `transforms.py`). The
join is therefore `substr(related_tax_id, 1, 8) = br_companies.cnpj_basico`,
and it belongs in a view: a partner pointing at a company we have not
ingested stays visible instead of silently becoming empty.

## 5. ClickHouse schema

`corpscout.br_company_relations`, shipped as migration `000208` (`000207` was
left free for another in-flight session).

```
country_iso2, source_slug, source_run_id, source_record_id
snapshot_year_month      String                  -- the partition it came from
cnpj_basico              String                  -- the company: subject of the edge
related_entity_kind      LowCardinality(String)  -- '1' company | '2' person | '3' foreign
related_name             String                  -- razao social, or the person's name
related_tax_id           String                  -- CNPJ, or MASKED CPF for a person
relation_code            LowCardinality(String)  -- qualificacao_socio, verbatim
relation_since           Nullable(Date32)
related_country          String
representative_tax_id    String                  -- the SECOND edge on this row
representative_name      String
representative_code      LowCardinality(String)
age_band                 LowCardinality(String)
resolved_at              DateTime64(3, 'UTC')

ENGINE = MergeTree
ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code)
```

Two shape decisions worth not rediscovering:

- **A row carries two edges, not one.** A foreign or incapacitated partner has a
  *legal representative* — another named person with their own qualification.
  Kept as columns on the same row because that is how the source publishes it;
  splitting into two rows is an interpretation a view can make. Consequence:
  "every person connected to company X" is a two-column query.
- **`related_tax_id` mixes CNPJ and masked CPF** in one column, discriminated by
  `related_entity_kind`. That is the honest cost of one edge model. Two nullable
  columns would query more tidily and model less faithfully.

Sort key is company-first because "who owns this company" is the primary access
path, and every member is non-nullable (`allow_nullable_key` is off).

`relation_code` decodes against RFB's `Qualificacoes` reference table, which the
module **already parses** — the decode is a view-time join, not an ingest-time
mapping.

## 6. Privacy — the design `brazil_rfb-design.md` deferred to

**Storage is verbatim, and that is defensible because minimization already
happened at the publisher.** RFB emits the CPF pre-masked (`***XXXXXX**`) as part
of a deliberate open-transparency dataset. We store what arrives and add nothing.

**Explicit non-goal: no de-anonymization.** We never reconstruct a full CPF from
the masked digits, and we never cross-reference person rows against other
datasets to resolve identity. Linking two companies through a shared partner is
in scope; establishing *who that human is* beyond what RFB published is not.
This belongs in the module docstring, because it is the kind of boundary that
erodes silently through well-intentioned enrichment.

> **SUPERSEDED 2026-07-29 — retention is now one year, not 90 days.** See
> `brazil_rfb_socios_history-design.md` §7. The 90-day window below was chosen
> when the archives' only purpose was reprocessing after a bug. Once
> `br_company_relations` became a connection *history*, the archives turned into
> the only path for rebuilding that history, and 90 days would have made it
> unverifiable after a quarter. Everything else in this section — the
> non-goal, the access and redaction rules, the two implementation notes — still
> holds.

The first draft of this section said retention "falls out favourably" because
the module replaces each snapshot and holds only the current month — and warned
that anyone later keeping monthly history would be changing the retention
posture, not just the storage strategy, and "should have to notice that."

**That happened, in this same branch.** The S3 snapshot step added to
`brazil_companies/rfb` exists so a partition stays rebuildable after RFB's
mirror drops a month — a good reason, and one that quietly turned
snapshot-replace into indefinite accumulation of person names, masked CPFs and
age bands. Nobody noticed until review. This is the deliberate answer:

- `family=socios` raw archives expire after **90 days**
  (`source.RFB_SOCIOS_RETENTION_DAYS`, applied as an S3 lifecycle rule by
  `rfb_socios_retention_rule()` whenever the bucket is ensured).
- The other nine families carry no personal data and are kept **indefinitely**,
  which is where the rebuildability guarantee actually lives.

Ninety days is chosen from purpose, which is the test that matters here.
ClickHouse serves only the latest snapshot, so an old raw archive has no
serving value; its only use is reprocessing after a bug or a layout correction,
and that need is concentrated in the last month or two. It gives an answerable
position — *we keep the source file for 90 days so ingest can be reprocessed* —
where "indefinitely, because storage is cheap" gives none.

**The honest cost, stated rather than discovered:** after 90 days a socios
archive is gone, so a year-old partition rebuilds only its nine non-personal
families. That is the trade, accepted knowingly.

Two implementation notes that are load-bearing rather than incidental:

- The object key is **`family=` before `snapshot=`**. S3 lifecycle rules filter
  by prefix, not glob, so `snapshot=*/family=socios/` cannot be expressed —
  only this order makes the rule a single line instead of a new rule each
  month. Changing the key order silently disarms the retention policy.
- The rule is applied **in code**, not by hand. This repo has no
  infrastructure-as-code, so a manually-applied policy would live nowhere,
  not survive a bucket being recreated, and never appear in review.

**Access**: this phase ships the edge table only, so nothing reaches the UI.
When it does, person rows sit behind whatever access rule governs person data
generally. **Redaction** is a view concern — dropping `related_name` where
`related_entity_kind = '2'` is one line, and storage stays faithful underneath.

**Purpose**: understanding corporate control and connection between companies,
from a dataset the Brazilian federal government publishes openly for exactly
that transparency purpose.

`faixa_etaria` (age band) is personal data under the same rules.

## 7. Currency

Not applicable. `Socios` publishes no monetary values — partner equity share is
not in this file.

## 8. Scheduling

None of its own. It is a family inside the existing monthly RFB partition and
runs with it. Adds ~20-25M rows to a partition already moving ~140M, in its own
file and pool, so it lengthens the month's wall-clock without blocking the
register chain.

`socios` must join `BrazilCompRfbStagePaths` so `previous_partition_cleanup`
deletes the stage file with the rest — otherwise every month strands a
multi-GB file.

## 9. Scope

**The edge table only.** No derived views, no UI.

Deliberate: we do not yet know the shape of this graph — how dense it is, how
long ownership chains run, how often one person appears across hundreds of
entities. Those facts decide whether a corporate-group view is a two-hop join or
a recursive traversal that needs guarding. One materialization answers all of
them, and designing the views first means designing them twice.

The counter-argument, recorded honestly: an edge table nobody queries is
invisible, and the value only appears with the views. They are the next piece of
work, not a cancelled one.

## 10. Verification

Gates, in order:

1. **Verify the 11-column layout against the real archive before writing any
   parsing.** §1 is written from knowledge of the RFB format, not from the file.
   Read every field before choosing — the lesson that cost time on Hilma's value
   and TED's monetary elements.
2. Fixture covering all three discriminator branches plus a row with a legal
   representative, so the two-edges-per-row case is exercised.
3. Transform runs against a **real DuckDB**, asserting resulting columns — not
   asserted SQL strings.
4. A row with empty optional fields lands `''`, never `NULL`. This only fails
   against real data with the native driver.
5. Contract test greps the migration for every export column.
6. Refuse-on-empty raises `ValueError`.

## 11. Things not to rediscover

```
Socios is NOT downloaded today   the fetcher discovers 9 families and socios is not
                                 one of them -- this starts at the archive step
Qualificacoes IS already parsed  the partner-role decode table is in the module
                                 already, not yet exported to ClickHouse
per-family DuckDB + pool         each RFB family has its own stage file and pool,
                                 so socios is isolated without a separate module
headerless Latin-1               fixed published column order, positional tuples
company_people_all               being retired (2026-07-28) -- BR person edges land
                                 in br_company_relations, NOT in a cross-country table
PGFN CPF rows                    dropped by `where length(cpf_cnpj_digits) = 14`.
                                 Co-responsible parties for a company's tax debt are
                                 often its administrators, so that filter discards a
                                 second company->manager signal. Separate decision.
```
