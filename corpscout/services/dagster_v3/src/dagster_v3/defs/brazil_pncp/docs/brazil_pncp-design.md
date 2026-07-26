# Brazil PNCP (government contracts) design doc

> Follows `docs/data-source-guidelines.md`; deviations are called out below.
> Status: **design only, nothing built.** Section 0 lists what must be verified
> before writing code — one of those answers can still change the ingest shape.

## 0. Open questions that gate the build

Three of the original four were settled by probe on 2026-07-26 (§2). One remains.

1. **What distinguishes `valorInicial` / `valorGlobal` / `valorAcumulado`?** One
   sampled record had `valorInicial == valorGlobal == 94390.8`, which
   distinguishes nothing. §7 states the intended choice; validate it against
   records where they differ before committing. This does not gate the ingest
   shape — only which column feeds `value_amount_original`.

**Resolved:** deep pagination works to the last page (page 341 of 340 returns a
clean `Página 341 inexistente`, not silent truncation), so monthly partitions
stand. Max `tamanhoPagina` is 500 — 1000 is rejected. Date bounds are inclusive
at both ends, verified arithmetically.

## 1. Source overview

- **Country / registry**: Brazil — Portal Nacional de Contratações Públicas
  (PNCP), the national procurement portal mandated by Lei 14.133/2021. Operated
  by Serpro. Covers federal, **state and municipal** procurement.
- **Module**: `defs/brazil_pncp/` · DuckDB file `data/brazil_pncp_source.duckdb`
  · pool `brazil_pncp_duckdb`
- **ClickHouse tables**: `corpscout.br_pncp_contracts` (new migration), plus the
  view `corpscout.br_government_contracts`
- **Datasets used**:

  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | contracts by publication date | `GET /api/consulta/v1/contratos?dataInicial&dataFinal&pagina&tamanhoPagina` | JSON | ~3.2M records, 41 fields | continuous | none |
  | contracts by update date | `GET /api/consulta/v1/contratos/atualizacao` | JSON | subset | continuous | none |

  OpenAPI spec: `https://pncp.gov.br/api/consulta/v3/api-docs` (12 endpoints).

- **Entity key**: `niFornecedor` — the supplier's **14-digit CNPJ**. Note this is
  an *establishment*, not a company; see §5.
- **Record count** (measured 2026-07-26, `totalRegistros` per month):

  ```
  2022-01        203      PNCP barely adopted
  2023-01      3,343
  2024-01     35,909
  2025-01    116,226
  2026-01    146,003
  2026-06    169,574     ~5.6k/day
  ```

  Extrapolated full history ≈ **3.2M contracts**. Volume is dominated by
  2025–2026; the early years are nearly free to backfill.

- **No bulk download exists.** Verified: `portaldatransparencia.gov.br`
  contracts download returns HTTP 500; `dadosabertos.compras.gov.br` publishes
  `_CSV` endpoints for pesquisa-preco/pgc/uasg/orgao but **not** for
  `modulo-contratos`, and covers federal purchasing only. Transparência Brasil
  and the Open Contracting Partnership publicly criticise PNCP for exactly this.
  The paginated API is the only path — this is a documented constraint, not a
  gap in our search.

## 2. Ingest mode (§2) — and *why*

- **Chosen**: partitioned API incremental — `MonthlyPartitionsDefinition` on
  `dataPublicacaoPncp`, `end_offset=1`, plus a **separate non-partitioned
  refresh job** on `/contratos/atualizacao`.
- **Why not bulk full-refresh**: no bulk file exists (§1). Why not
  single-request: 3.2M records over 6,400 requests cannot be one call.
- **Why two date axes.** Contracts are amended — the payload carries
  `numeroRetificacao`, `dataAtualizacaoGlobal` and `temRemanejamento`. A
  publication-date-only pipeline would ingest each contract once and never see
  its amendments, so a contract whose value was revised keeps the original
  figure forever. This is the same failure that produced wrong ESEF revenue
  figures until amendment documents were read, and it is cheap to avoid:
  backfill by publication, then a daily job re-reads `/contratos/atualizacao`
  for a trailing window and upserts. ReplacingMergeTree on
  `dataAtualizacaoGlobal` collapses the versions.
- **Backfill policy**: `BackfillPolicy.multi_run(max_partitions_per_run=1)` with
  the module pool, per guidelines. ~55 monthly partitions, ~340 requests each,
  ~6,400 for full history.

### Pagination and date bounds — measured 2026-07-26

```
tamanhoPagina=1000            HTTP 400  "Tamanho de página inválido"
tamanhoPagina=500             accepted            <- the maximum
2026-06, page 1     of 340    500 records
2026-06, page 100   of 340    500 records         <- no depth cap
2026-06, page 340   of 340     74 records         <- exact remainder
2026-06, page 341   of 340    HTTP 400 "Página 341 inexistente."
```

Deep pagination works to the end, and the arithmetic is self-consistent:
169,574 records over 340 pages leaves 74 on the last, which is what came back.
Past the end the API says so explicitly rather than returning an empty page, so
a pager can trust `totalPaginas` and still has an unambiguous stop condition.

Date bounds are **inclusive at both ends**, verified by arithmetic rather than
assumption:

```
2026-06-01 alone      5,936
2026-06-02 alone      8,888
2026-06-01..06-02    14,824  = 5,936 + 8,888   exactly
```

So a monthly partition is [first day, last day] with no overlap adjustment, and
consecutive partitions neither drop nor double-count a day.

### Rate limiting

Measured 2026-07-26: roughly 15 requests in a few minutes returned **HTTP 429**
(`Limite de Requisições Excedido`). Recovery inside ~2 minutes.

**The response carries no rate-limit headers at all** — no `X-RateLimit-*`, no
`Retry-After`. A client cannot be told when to retry and must infer it. The
exact requests-per-minute ceiling was deliberately **not** measured: finding it
means hammering a public government service until it refuses, and adaptive
backoff makes the number unnecessary.

Mitigation is the house pattern rather than custom code:
`dlt.sources.helpers.requests` already retries 429 with backoff, and the
guidelines already mandate it over plain `requests`. Pace conservatively and
treat 429 as expected, not exceptional.

## 3. Loading (§3)

- **Reader**: fetch pages with `dlt.sources.helpers.requests`, append each
  page's `data` array verbatim to a per-partition **JSONL file**, then load with
  DuckDB `read_json`. Not a Python-dict-per-row dlt resource: the guidelines
  call that the slow path, and DuckDB's reader is set-based.
- **Why not `read_json` straight off the URL**: the endpoint is paginated and
  rate-limited, so page fetching has to be explicit anyway. Writing pages to
  JSONL keeps the raw response as the checkpoint — a partition that fails at
  page 300 does not re-fetch pages 1–299.
- **Staging shape**: `raw_contracts` (all 41 fields as VARCHAR, plus
  `source_run_id`, `source_url`, `source_retrieved_at`, `source_line_number`),
  then `contract_candidates` with typed and normalised columns. Raw JSON stays
  in DuckDB only and is **not** exported (§5).
- **Nested objects**: `orgaoEntidade`, `unidadeOrgao`, `orgaoSubRogado`,
  `unidadeSubRogada` are objects. Flatten the fields used (buyer CNPJ, name,
  `poderId`, `esferaId`, UF, municipality) in DuckDB SQL; keep the whole object
  in the raw table.

## 4. Transform (§5)

- **Mechanism**: set-based DuckDB SQL. No dbt — nothing here earns it.
- **Shape**: register row-map. One row per contract per supplier, which is what
  the endpoint already returns; there is no separate winners table to join, so
  this is shaped like Sweden's UHM rather than TED/Hilma.
- **Key SQL ideas**:
  - `niFornecedor` normalised to digits only and joined to
    `br_establishments.cnpj`. `supplier_cnpj` keeps the 14-digit establishment;
    `company_id` is the **joined** `cnpj_basico`, not a substring of the input.
    The difference is validation: a substring proves only that some company owns
    that prefix, so a CNPJ with a valid base and an establishment we do not have
    would be silently attributed to the parent. Taking company_id out of the
    join means it is populated only when the establishment genuinely exists.
  - `tipoPessoa` retained; matching is restricted to `'PJ'` (§5).
  - `numeroControlePNCP`, `anoContrato`, `sequencialContrato` and the buyer CNPJ
    give the source document URLs (§5).

## 5. ClickHouse schema — and DDL deviations

- **Table + grain**: `br_pncp_contracts`, one row per contract per supplier.
- **Engine**: ReplacingMergeTree(`dataAtualizacaoGlobal`) — amendments replace
  earlier versions of the same contract.
- **`ORDER BY`**: `(company_id, source_notice_id, sequencial_contrato)`.
  Non-nullable, per the `allow_nullable_key` constraint.

### The identity decision (the important one)

PNCP awards to a **14-digit CNPJ** (an establishment). `br_companies` keys on
**`cnpj_basico`, 8 digits** (the company). So:

```
niFornecedor  57423612000104   ← the establishment that won the contract
cnpj_basico   57423612         ← the company we attribute it to
```

**The join needs no lookup table.** The 8-digit base *is* the first 8 characters
of the 14-digit CNPJ, so matching is a prefix against the company register
directly, verified 2026-07-26:

```sql
substring(niFornecedor, 1, 8) = br_companies.cnpj_basico
-- 57423612000104 -> 57423612 -> RLIMP SERVICOS LTDA
```

That validates the supplier against 68.6M companies in one join and works
whether a matriz or a filial signed. `br_establishments` (71.9M rows) is
**not** needed for matching — only later, if the specific branch's name or
address is wanted.

**Do not join `br_companies.headquarters_cnpj`.** It is 14 digits, 100%
populated, and looks like the obvious match for `niFornecedor` — but it is the
company's *headquarters* establishment (`ordem 0001` for 99.98% of rows), while
`niFornecedor` is whichever establishment signed the contract. Matching on it
silently drops every contract won by a branch, and the loss is invisible: the
unmatched rows look like suppliers missing from the register rather than a wrong
key. It would appear to work in spot checks, because a sampled supplier is
usually a matriz.

Both identifiers are unique — of different things. Measured in our own data:

```
br_companies       68,629,147 rows = 68,629,147 distinct cnpj_basico
br_establishments  71,874,448 rows = 71,874,448 distinct 14-digit cnpj
                                     across 68,629,147 companies
```

97.96% of companies (67,227,845) have exactly one establishment, so for almost
every entity the distinction is invisible. It is the remaining 2.04% —
1,401,302 companies — where it decides correctness, and those are the large
organisations most likely to win public contracts. One company has 9,999
establishments, the ceiling of the 4-digit `cnpj_ordem` field. Keyed on 14
digits, such an organisation would appear as thousands of separate companies and
its government business would never aggregate.

**Decision**: `company_id` is the 8-digit base, so contracts roll up to the
company, matching every other country and `br_companies`. The 14-digit value is
kept as its own column — dropping it would lose which branch actually won, and
keeping source detail is the standing rule.

**`tipoPessoa`**: `PJ` is a legal entity, `PF` a natural person. PF suppliers
must not be matched against a company register — Sweden hit exactly this and
classifies such rows `person_keyed`. Mirror that: keep the row, mark it
ineligible, do not fabricate a `company_id`.

### Deviations

- No `contracted` flag equivalent: every row in `/contratos` is an executed
  contract, so there is nothing to filter.
- `source_url`: PNCP publishes a real per-contract address, unlike Sweden's UHM.
  Both verified HTTP 200 on 2026-07-26:
  - human: `https://pncp.gov.br/app/contratos/{buyer_cnpj}/{ano}/{sequencial}`
  - API: `https://pncp.gov.br/api/pncp/v1/orgaos/{buyer_cnpj}/contratos/{ano}/{sequencial}`

  Store the human one, as with TED and Hilma.
- **Export subset**: `BR_PNCP_CONTRACTS_EXPORT_COLUMNS` drops `raw_contract` and
  `source_payload_hash` per guidelines.

## 6. Translation (§8) — `defs/brazil_pncp/translation.py`

| label | source_column | mechanism | notes |
|---|---|---|---|
| contract object | `contract_object_original` | LLM | `objetoContrato`, free Portuguese text |
| contract type | `contract_type_original` | static dict | `tipoContrato`, coded |
| process category | `process_category_original` | static dict | `categoriaProcesso`, coded |

`_en` is served by the `brazil_pncp_translated` join view over `text_translations`,
not by base-table columns. Deliberately **not** translated: supplier and buyer
names, `processo`, contract numbers — proper nouns and identifiers.

Note `objetoContrato` is high-cardinality free text at ~3.2M rows. Translating
all of it is expensive and probably not warranted up front; translate on demand
or restrict to contracts above a value threshold, and record which.

## 6b. Contacts (§8b)

**None.** The contracts payload carries no email, phone or website for either
supplier or buyer — the 41 fields are contract attributes and party identifiers
only. Brazilian company contacts already arrive via `br_company_contacts`
(119.4M rows) from the CNPJ register. No `br_pncp_company_contacts` table.

## 7. Currency (§7)

- **Native currency**: BRL. The payload has no currency field — values are BRL
  by definition of the register. Store `'BRL'` explicitly rather than leaving it
  empty, so the view's `value_currency` is truthful.
- **All five value fields are stored**, not one. `br_pncp_contracts` keeps
  `valor_inicial`, `valor_global`, `valor_acumulado`, `valor_parcela` and
  `numero_parcelas` as their own columns. Choosing one at ingest and dropping
  the rest is the lossy move being removed everywhere else in this design — the
  source table keeps what the register publishes, and the view selects.

  The API documents none of them: `RecuperarContratoDTO` types all five and
  gives no description. Their meaning is the Brazilian public-contracting
  convention, not a documented contract:

  | field | meaning by convention |
  |---|---|
  | `valorInicial` | value as originally signed |
  | `valorGlobal` | total over the full term, usually `valorParcela × numeroParcelas` |
  | `valorAcumulado` | running total after amendments (*aditivos*) and adjustments (*reajustes*) |

  Two falsifiable predictions to check against a page of real records, since a
  sampled contract had `valorInicial == valorGlobal` and so confirmed nothing:
  `valorParcela × numeroParcelas == valorGlobal` for instalment contracts, and
  `valorAcumulado >= valorInicial` wherever `numeroRetificacao > 0`. If the
  second holds it also demonstrates that the amendment-refresh job in §2 is
  necessary in fact, not just in theory.

- **The view names the column it used.** `value_amount_original` is
  `valorGlobal`, and the view carries `value_source_field = 'valorGlobal'`
  alongside it, so a displayed figure states its own origin. A value with no
  stated source cannot be checked against the register — the same reason
  `source_url` exists, applied to fields instead of documents.

  **This generalises to the countries already built**, and is worth adding
  there: TED's figure is `awarded_amount`, Hilma's is `procurement_value` (and
  is notice-level), Sweden's is absent entirely. Today the UI can say only
  "Value" and, for Hilma, "whole notice". With `value_source_field` it can name
  the register's own field, which is what someone verifying a number needs.

- Carries `value_amount_original` + `value_amount_usd` +
  `fx_rate_to_usd/_date/_source`, keyed on `dataAssinatura` (falling back to
  `dataPublicacaoPncp`), converted in a separate `apply_brazil_pncp_usd_conversion`
  step via the shared `ExchangeRateClient`. Never inline with extraction.

**This is the first country where the national register publishes a value
attributable to a single winner.** Sweden's UHM publishes none at all; Finland's
Hilma publishes only a notice-level figure that repeats across winners. PNCP's
value is per contract per supplier, so it populates `value_amount_*` properly
rather than `notice_value_*`.

## 8. Scheduling (§9)

- `brazil_pncp_backfill_job` — the monthly partitions. **Run once, and never
  scheduled.** It loads 2022-01 to the present and afterwards exists only to
  repair a specific month. Re-fetching a 340-page month to pick up a single
  day's contracts would be absurd when a day is 12 pages.
  Launch from the UI, not the CLI (the code-location caveat in the guidelines).
- `brazil_pncp_daily_job` — daily, and the only thing on a schedule once the
  backfill has run. It reads a trailing 7-day window from **both** endpoints:
  `/contratos` by publication date for newly published contracts, and
  `/contratos/atualizacao` by update date for amendments to older ones.

  Querying both is deliberate belt-and-braces. It is not established whether
  `/contratos/atualizacao` also returns newly *published* contracts — probably
  it does, since a new contract's `dataAtualizacaoGlobal` is its creation time —
  and reading both means never having to find out. A day is ~5,600 records, so
  the pair costs ~24 requests. ReplacingMergeTree collapses whatever overlaps.

  The 7-day window rather than 1 day absorbs a missed run without needing a
  catch-up path.
- `br_government_contract_signals_clickhouse` — the coverage asset, downstream,
  following the existing per-country pattern.
- Cron minute staggered against other sources; module pool serialises its own
  steps.

## 9. `br_government_contracts` view

A single-branch view, no `UNION ALL`: Brazil is not in the EU and has no TED
notices, making it the mirror image of Norway (TED only, no national register).
The per-country view design already allows this — a country is the list of
sources it actually has.

Columns follow the established shape. Country-specific columns (`emenda_parlamentar`,
`fruto_adesao`, `numero_parcelas`, `subcontractor` fields) may be carried in
addition — `merge()` tolerates ragged views — but **must be declared
`Nullable`**, so that "this country does not publish it" reads as NULL rather
than a type default indistinguishable from a real `0`.

`directive_governed` has no meaning outside the EU procurement directives:
emit `''` (unknown), not `'no'`. Claiming "below EU threshold" for a Brazilian
contract would be nonsense.

## 9b. UI: establishments are shown, not hidden

Storing the establishment is pointless if every surface collapses it — but only
one surface is in scope here.

### The country company list — deliberately not designed yet

Brazil's register could support a grouped company list: `br_establishments` is
71.9M rows keyed on the 14-digit CNPJ, loaded today, with its own address and
status per branch. Grouping by company with branches nested, a toggle to flatten
that keeps a parent-company column — all buildable now, without any PNCP data.

**It is deliberately out of scope.** `/countries/:country/companies` is shared by
all ten countries, and Brazil is currently the only one with a branch register of
this shape. Designing that surface around a single country's structure is how it
ends up fitting none of them — the same reason the unified companies table is
being left alone until more countries have proper per-country tables to
generalise from. Revisit when a second and third country show what the shared
shape actually is.

What is *not* deferred is the storage: `supplier_cnpj` is kept regardless, so
whenever that list is designed the data is already there to group by.

### A company's government contracts — needs this source

On the company detail page, group contracts by the winning establishment:

```
RLIMP SERVICOS LTDA  (57423612)
  matriz  57423612000104     12 contracts
  filial  57423612000201      3 contracts
                             15 total for the company
```

This is the question the 14-digit key uniquely answers — *which of our branches
sells to government* — and it is invisible if contracts are only ever attributed
to the parent. It is also the reason `supplier_cnpj` is stored rather than
derived: a rollup can be computed from the establishment, but the establishment
cannot be recovered from a rollup.


## 10. Verification

- **Tests**: `tests/test_brazil_pncp_*.py` — CNPJ normalisation and the 14→8
  split, PJ/PF classification, value/date typing, export-vs-migration column
  contract, view shape, schedule registration.
- **Live**: migrate → backfill one month (start with 2024-06, ~36k records, big
  enough to be real and small enough to be cheap) → check row counts against
  `totalRegistros` for that range → spot-check a known contract's
  `valorGlobal × rate` and that its `source_url` returns 200 → confirm
  `company_id` join rate against `br_companies`.
- Run `scripts/dagster-health-check.py` afterwards.

## 11. Issues found during investigation

- **No bulk download, and this is known.** Confirmed against two alternative
  portals before concluding. Do not spend more time looking.
- **429 with no headers.** The absence of `Retry-After` is the real problem, not
  the limit itself — a client has nothing to obey. Adaptive backoff only.
- **The rate limit blocks investigation, not just ingestion.** Two of the four
  open questions in §0 are unanswered *because* probing tripped the limit. Budget
  for slow, spaced-out verification rather than an interactive session.
- **14 vs 8 digit CNPJ.** The obvious join (`niFornecedor` = company id) is
  wrong; it is an establishment. Silent, because the string still looks like a
  CNPJ and would simply match nothing.
- **`headquarters_cnpj` is a worse trap than that one**, because it half-works.
  It is 14 digits, 100% populated, and matches `niFornecedor` exactly whenever
  the matriz signed the contract — which a spot check will usually hit. Every
  branch-won contract is dropped, and the loss reads as suppliers missing from
  the register rather than a wrong key.
- **`Inte direktivstyrd`-class trap, Brazilian edition**: `tipoPessoa` is a
  two-letter code where `PF` and `PJ` differ by one character. Match exactly.
