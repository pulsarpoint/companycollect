# Procurement sources: independent and complete, before unification

**Goal:** Every procurement source ingested completely, shown as itself, and
linked to companies where its register allows — *before* deciding how one
contract seen in several sources becomes one thing.

**Ordering principle (from the user, 2026-07-27):** sources first, unification
last. This reverses what was done in this session, and the reversal is
evidence-backed: unifying early forced questions the raw data had not yet
answered, and two wrong answers were shipped and later corrected (§4).

---

## 1. Where things stand

### Built and live

| | source | rows | valued | notes |
|---|---|---|---|---|
| SE | `sweden_uhm_procurement` | 96,094 | 0 | UHM publishes no value at all — 44 CSV columns, none monetary, confirmed against the live file |
| SE | `ted_procurement` | 146,605 | 33,176 | landed 2026-07-27 |
| FI | `finland_hilma_procurement` | 9,275 | 4,136 | `lots_value` at lot grain |
| FI | `ted_procurement` | 39,314 | 14,000 | |
| NO | `ted_procurement` | 20,559 | — | 5,024 companies |
| BR | `brazil_pncp_procurement` | 203 | 203 | **one month only** (2022-01) |

Views: `{se,fi,no,br}_government_contracts` + per-country summaries, plus a
passthrough `company_government_contract_summary` that `companies_all` still
joins. There is deliberately **no** cross-country row-level view (removed in
`f9fe8272` — it was `merge()` over a name pattern, read by nothing but the
summary).

UI: Contracts tab per country, contract detail page with winners linked to
companies and every source document linked. The tab is gated on which
`*_government_contracts` views exist, so a new country's view lights it up with
no frontend change.

### Not built

- Brazil: full backfill (54 partitions), BRL→USD conversion, the daily job
- Norway: Doffin entirely (design doc written, one blocker — §5)
- Source metadata tables (§3)
- The procurement sources page and the per-country "where to find open tenders"
  information (§3)

---

## 2. Measurements worth not rediscovering

Each of these cost real time or rate-limited requests.

### Brazil PNCP
```
API            pncp.gov.br/api/consulta/v1/contratos   (no auth)
bulk download  DOES NOT EXIST — confirmed against portaldatransparencia (500)
               and dadosabertos.compras.gov.br (no CSV for modulo-contratos),
               and publicly criticised by Transparência Brasil. Do not look again.
page size      500 max (1000 rejected)
pagination     no depth cap — page 341 of 340 errors explicitly, not silently
date bounds    inclusive both ends (5,936 + 8,888 = 14,824 exactly)
rate limit     429 with NO Retry-After and no rate-limit headers at all
throughput     ~6 pages/min sustained, stable across a 205-page month
volume         ~3.2M contracts; 2024-06 alone is 102,250 over 205 pages
full backfill  ~6,400 requests ≈ 19 hours
identity       substring(niFornecedor,1,8) = br_companies.cnpj_basico
```

### Norway Doffin
```
API            api.doffin.no/public/v2/search   (Ocp-Apim-Subscription-Key)
keys           DOF_NOTICE_PRIM_API_KEY / _SEC_API_KEY in dagster_v3/.env
volume         157,050 notices; 31,095 ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT
cap            numHitsAccessible = 1,000 on EVERY query, regardless of filter
winners        lots[].winner[].organizationId — joins no_companies untouched
               (verified: 979750730 → KONGSBERG MARITIME AS)
type filter    works, cuts 157,050 → 31,095
date filters   PARAMETER NAMES UNKNOWN — and unrecognised params are silently
               ignored, returning the full set looking filtered. Must be read
               from the API definition, never guessed.
```

### Cross-source matching
```
FI   Hilma publishes ted_number → 2,237 contracts matched across both registers
SE   no published cross-reference → buyer/date/title hash → 0 matches of 242,699
```

---

## 3. Phase 1 — each source independent and complete

Deliverables, in rough dependency order. Each is shippable alone.

### 3.1 Brazil PNCP: finish the ingest

- **Full backfill**, 54 monthly partitions, from the Dagster UI on prod, not
  locally, not the CLI. ~19 hours. Watch a modern partition (2025-01, 233 pages)
  before releasing the rest.
- **Add a retry policy** to `brazil_pncp_raw_pages_s3` first:
  `dg.RetryPolicy(max_retries=3, delay=60, backoff=EXPONENTIAL)`. A transient
  failure already occurred within 80 pages; over ~7,000 they will be routine,
  and the per-page cache means a retry resumes rather than re-fetches. Without
  this an overnight backfill is found stalled rather than done.
- **BRL→USD** (`apply_brazil_pncp_usd_conversion`), separate step keyed on
  `data_assinatura` falling back to `data_publicacao_pncp`, via the shared
  `ExchangeRateClient`. Until it runs, every Brazilian value is BRL with a NULL
  USD, so Brazil cannot be compared or ranked against other countries.
- **The daily job**: trailing 7 days from **both** `/contratos` (new) and
  `/contratos/atualizacao` (amendments). Reading both is deliberate — it is
  unestablished whether the update endpoint also returns newly published
  contracts, and ~24 requests/day is cheaper than finding out.
- **Settle the value question** from stored data, not new requests: which of
  `valorGlobal` / `valorInicial` / `valorAcumulado` is *the* contract value.
  Three sampled contracts had the first two identical. Test
  `valorParcela × numeroParcelas == valorGlobal` and
  `valorAcumulado >= valorInicial where numeroRetificacao > 0`. This is now
  cheap because the raw JSON is snapshotted in S3.

### 3.2 Norway Doffin: build it

Blocked on one thing: the date filter parameter names (§2). Get them from
`https://dof-notices-prod-api.developer.azure-api.net/apis`. Everything else in
`defs/norway_doffin/docs/norway_doffin-design.md` is measured.

Doffin is the cheapest national source to add — `organizationId` joins
`no_companies` with no transformation, unlike Sweden's person/company problem or
Brazil's 14→8 digit resolution.

Second open question: whether any **realized** value exists, and at what grain.
`estimatedValue` is an estimate at notice level and belongs in neither
`value_amount_*` nor `notice_value_*` — both hold realized figures. **Search
inside `lots[]` before concluding no value exists**: that is exactly the mistake
made with Hilma, whose better field sat beside the one first used.

### 3.3 Source metadata in the database

A table per country describing the register it reads, so the UI stops depending
on Python constants. Columns to carry at minimum:

```
country_code, source_slug, register_name, operator,
homepage_url, api_or_download_url, licence,
coverage_description,          -- what this register does and does not include
open_tenders_url,              -- where someone wanting to BID goes
notes
```

The last one matters and is not currently anywhere: everything built so far
answers "what was awarded", nothing answers "where do I find open offers".

`company_signal_coverage` already holds the per-country *status* (span, sources,
caveat) and is **unused in the UI** — the source panel should read it rather
than duplicating it.

### 3.4 Show each source as itself — `/procurements/{source}`

**This is the centrepiece of Phase 1, not a footnote.** One page per source,
listing everything that source publishes, read from the *source tables* rather
than the country views:

```
/procurements/ted                    ted_notices + ted_notice_winners
/procurements/sweden-uhm             se_uhm_procurement_awards
/procurements/finland-hilma          fi_hilma_notices + fi_hilma_notice_winners
/procurements/brazil-pncp            br_pncp_contracts
/procurements/norway-doffin          (once built)
```

Clicking a procurement opens what we actually pulled from that API: the fields
as the source publishes them, in its own shape and vocabulary, not flattened to
the canonical columns. The contract detail page already does this per contract
with `SELECT *`; these pages do it for the source as a whole.

**Why this matters more than it looks.** These pages are not country-scoped, and
that is the point:

- **They make §4.1's dropped rows visible.** A country view shows only contracts
  won by that country's companies, so the 4,304 cross-border wins appear on no
  page at all today. A TED page shows every TED notice — including the Danish
  company that won in Sweden — because it is showing TED, not Sweden.
- **They show the source's own fidelity.** The country views expose 26 canonical
  columns; TED publishes far more, Hilma has trilingual names and NUTS regions,
  PNCP has 41 fields including subcontractors and parliamentary amendments. The
  canonical shape is a projection *for comparison*, and comparison is Phase 3's
  problem. Phase 1 shows what is there.
- **They are how a source is verified.** "Did we pull TED correctly" is
  answerable by looking at the TED page against ted.europa.eu. It is not
  answerable from a country view, which has already filtered, joined and
  projected the data three times.
- **They do not depend on company matching.** A procurement with an unmatched
  winner still belongs on its source page. Only the *link to a company* is
  conditional, exactly as the contract detail page already handles winners with
  no registry match.

**Shape per source page:**

```
header      what this source is, who runs it, what it covers, licence,
            where open tenders are published  (from the metadata table, §3.3)
coverage    date span, row counts, last refreshed  (company_signal_coverage)
list        the source's own natural grain, its own columns, filterable by
            country and date
detail      every field the source publishes for that record, plus a link to
            the source document, plus links to any companies matched
```

The country Contracts tab keeps its source panel (from
`company_signal_coverage` plus §3.3's metadata), with each source badge linking
through to its `/procurements/{source}` page. The country page answers "what did
this country buy"; the source page answers "what does this register contain and
did we read it correctly". Both are needed and they are not the same question.

---

## 4. Defects found, and what they imply

### 4.1 Cross-border winners are silently dropped — OPEN

```
notice country   domestic   FOREIGN
SE                151,806     2,599
FI                 45,088     1,012
NO                 21,621       693
                            ─────────
                              4,304  dropped, invisibly
```

Every country view filters `upper(winner_country) IN ('SE','SWE')` and joins
only that country's register. A Danish company winning a Swedish contract
appears nowhere — not in `se_government_contracts`, and not in a Danish view
because Denmark has no register loaded. Unlike a NULL, nothing marks the gap.

The root cause is a conflation baked into the view:

```
contracts LET BY this country's buyers      ← a procurement fact
contracts WON BY this country's companies   ← a company fact
```

They coincide for ~98% of rows, which is why it held. **This is the question
Phase 3 exists to answer** and cannot be fixed by editing a filter, because the
winner's register may not be loaded at all.

**Do now, cheaply:** state it in the coverage caveat, so the gap is visible while
the real answer waits. The source pages in §3.4 also surface these rows for the
first time — a TED page shows every TED winner regardless of nationality, which
is the difference between a gap that is documented and one that is observable.

### 4.2 Sweden's two sources are not deduplicated — OPEN

Zero cross-source matches across 242,699 rows. A Swedish contract in both
registers counts as two, and the caveat does not say so. The per-contract
collapse built in migration 195 never fires for Sweden because `contract_key`
never matches.

**Do first, before any provenance UI ships** — a page whose purpose is
explaining where data comes from must not carry a caveat that misleads. Check
whether Swedish TED notices carry a national reference to UHM procurement ids
(Finland's `ted_number` gave 2,237 matches); if none exists, say plainly that
Swedish counts are of notices, not distinct contracts.

### 4.3 Corrected this session, recorded so they are not repeated

- **Hilma's value.** `procurement_value` (notice-level) was used while
  `lots_value_amount_original` sat beside it at lot grain with better coverage.
  Lesson: read every candidate field before choosing, and check whether one
  varies at a finer grain than another.
- **Value double-counting.** Summaries summed across sources; a contract in two
  registers had its value counted twice. Latent only because Hilma contributed
  nothing. Fixed for all countries in migration 195.
- **`headquarters_cnpj`.** 14 digits, fully populated, matches whenever a matriz
  signed — a trap that passes spot checks while dropping every branch-won
  contract.
- **Unrestricted join on `br_companies`.** ClickHouse materialises the whole
  right side; 68.6M rows to resolve a few hundred thousand suppliers. Took over
  three minutes for four lookups before being restricted.
- **Raw data thrown away.** The Brazil module fetched, normalised, then deleted
  its pages — in the one pipeline where re-fetching costs 6,400 rate-limited
  requests. Now snapshotted to S3 first.

---

## 5. Phase 3 — unification, deliberately last

Not to be designed until Phase 1 is done. The questions it must answer:

1. **What is a contract, across sources?** Finland's published `ted_number`
   works; Sweden's name hash does not. Is there a general answer, or is it
   per-source-pair?
2. **What is a contract, across borders?** A notice let by a Swedish buyer and
   won by a German company belongs to whom? Today: nobody.
3. **What identity do companies have without a register?** The 4,304 dropped
   rows are mostly companies whose national register is not loaded. A name and a
   country code may be all there is.
4. **Does the cross-country view come back?** It was removed because nothing
   read it and it exposed every column of every country. If it returns it should
   be a narrow, explicit contract of genuinely comparable fields — not
   everything unioned.

Per-country views are not a stepping stone to a unified table; they are the
design. Countries publish genuinely different things, and one object spanning
them is either unmanageably wide or lossy — the lossy version is what the old
materialized evidence table was, and it is how contract value went missing for
every country at once.
