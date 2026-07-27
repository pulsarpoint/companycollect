# Norway Doffin (government contracts) design doc

> Follows `docs/data-source-guidelines.md`; deviations are called out below.
> Status: **design settled, build unblocked.** Section 0 records how each of the
> four gating questions was answered on 2026-07-27.

## 0. The questions that gated the build — all four resolved

They were answered from the **Azure APIM definition** rather than by probing,
which matters because a guessed parameter here fails silently (§7). The portal
UI is a SPA that renders nothing useful, but its content API serves the
definition anonymously:

```
https://dof-notices-prod-api.developer.azure-api.net/developer/apis?api-version=2022-04-01-preview
  -> Public API id 651eeff38be0c56022ac741d
.../developer/apis/651eeff38be0c56022ac741d/operations?api-version=2022-04-01-preview
.../developer/apis/651eeff38be0c56022ac741d/schemas/664e473e07d9191bc00c8c2e?api-version=2022-04-01-preview
```

### 0.1 Date filter parameters — `issueDateFrom` / `issueDateTo`

Format `yyyy-mm-dd`, inclusive, and they filter on **issue date, not
publication date**. There is no publication-date filter at all, so partitions
must be keyed on issue date. Verified live — the counts move, which is the
check that distinguishes a real filter from an ignored one:

```
type=AWARD, no date filter                 numHitsTotal 31,097  accessible 1,000
type=AWARD, issueDate 2025-01-01..01-31    numHitsTotal    311  accessible   311
type=AWARD, issueDate 2024-06-01..06-30    numHitsTotal    343  accessible   343
type=AWARD, issueDate 2025-01-15..01-15    numHitsTotal     27  accessible    27
```

The three parameters guessed earlier (`publishedFrom`, `fromDate`,
`publicationFromDate`) do not exist. `numHitsPerPage` maxes at **100**, not the
20 in the example. Full parameter list: `numHitsPerPage`, `page`, `sortBy`,
`searchString`, `type`, `status`, `cpvCode`, `location`, `issueDateFrom`,
`issueDateTo`, `estimatedValueFrom`, `estimatedValueTo`.

### 0.2 Realized value — **yes, at per-winner grain, but only in the XML**

This is the most consequential finding, and it inverts the assumption the
question was written under. **The search endpoint publishes no realized value —
only `estimatedValue`.** But `GET /v2/download/{doffinId}` returns the full
**eForms UBL XML**, and that carries realized figures:

| eForms | XPath | grain | in sample of 20 |
|---|---|---|---|
| BT-720 Tender Value | `efac:NoticeResult/efac:LotTender/cac:LegalMonetaryTotal/cbc:PayableAmount` | **(notice, lot, winner)** | 10 |
| BT-161 Notice Value | `efac:NoticeResult/cbc:TotalAmount` | notice | 7 |
| BT-27 Estimated Value | `cbc:EstimatedOverallContractAmount` | notice | ~50% |

BT-720 is exactly the grain the canonical `value_amount_*` columns want, and it
resolves to a **named winner with an org number** through
`LotTender → TenderingParty → Tenderer → Organization →
cac:PartyLegalEntity/cbc:CompanyID`. Measured on notice `2025-118285`:

```
TEN-0001  NOK  8,000,000  Endotech AS          958103425
TEN-0002  NOK    200,000  KCI MEDICAL AS       988885614
TEN-0005  NOK 25,000,000  Medivatus AS         819807892
TEN-0008  NOK 30,000,000  Stryker AB           556516-1352   <- Swedish org no.
```

So Doffin is **stronger** than Hilma here, not weaker: Hilma had lot grain but
no per-winner figure, and Doffin has both. That last row also shows §4.1's
cross-border problem is present in Doffin from day one.

The estimate is not a substitute for the realized value — notice `2026-109693`
carries `estimatedValue` 2,500,000 against a BT-161 of 1,485,571. Had this been
built from the search endpoint alone, every Norwegian contract value would have
been an estimate labelled as a contract value.

`lots[]` in the **search** response really does carry no value — only
`heading`, `description`, `winner[]`, confirmed against both the schema
(`PublicNoticeHitDto`) and 100 live notices. The richer `NoticeLot` schema in
the definition (with `estimatedValue`, `awardResult`, `contract`,
`receivedSubmissions`) belongs to a `PagedNoticeDto` endpoint that is **not
exposed on the public path** — `/v2/notices-with-dates` and variants all 404.

### 0.3 `numHitsAccessible` is a result ceiling — `min(numHitsTotal, 1000)`

It falls below 1,000 whenever the total does (311, 343, 27 above), so it caps
*results*, not pagination depth. Date slicing therefore suffices, with room to
spare: the busiest month measured is **378** award notices, less than 40% of the
cap.

```
award notices per year, by issue date
2018 2,864   2019 3,106   2020 3,210   2021 3,736   2022 3,764
2023 3,623   2024 3,204   2025 3,237   2026 1,923 (partial)
nothing before 2018 -- 2006..2013 all return 0
busiest months: 2023-12 378, 2024-12 337, 2025-12 299, 2025-06 293
```

### 0.4 Rate limits — 429 with no `Retry-After`, clearing in seconds

Probed deliberately rather than discovered during a backfill:

```
/v2/download   40 requests back-to-back = 125 req/min, zero 429
/v2/search     429 after ~14 requests at ~1 req/s
429 response   no Retry-After, no X-RateLimit-* headers, empty body
recovery       200 again on the next poll, <15s later
downloads kept returning 200 while search was throttled
```

It is a **short-window burst limit on search**, not a quota — the same
"no Retry-After, back off blindly" shape as PNCP, which is exactly what
`dlt.sources.helpers.requests` handles. Search is the cheap side of this ingest
anyway (4 pages per month against ~300 downloads), so a throttled search costs
nothing.

**One nuance worth keeping:** unrecognised *parameter names* are ignored
silently (§7), but invalid *enum values* are rejected with a 404 —
`sortBy=NOT_A_REAL_VALUE` and `sortBy=ISSUE_DATE_ASC` both 404, while
`sortBy=PUBLICATION_DATE_ASC` works. The `sortBy` enum in the schema document is
stale; the one in the operation description is live.

## 1. Source overview

- **Country / registry**: Norway — Doffin, the national procurement notice
  database, operated by DFØ. This is the register the current Norwegian coverage
  caveat names as missing.
- **Module**: `defs/norway_doffin/` · DuckDB file `data/norway_doffin_source.duckdb`
  · pool `norway_doffin_duckdb`
- **ClickHouse tables**: `corpscout.no_doffin_notices` (new migration), feeding
  the existing `corpscout.no_government_contracts` view as a second branch.
- **Datasets used**:

  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | notice search | `GET api.doffin.no/public/v2/search` | JSON | 157,055 notices | continuous | **yes** |
  | notice document | `GET api.doffin.no/public/v2/download/{doffinId}` | eForms UBL XML | ~15–70 KB each | per notice | **yes** |

  Both are needed, and that is not an optimisation choice: the search gives the
  notice list and its winners, the download gives the **money** (§0.2). Neither
  alone produces a usable contract row.

  Auth is an Azure API Management subscription key sent as
  `Ocp-Apim-Subscription-Key`. Registration:
  `https://dof-notices-prod-api.developer.azure-api.net/signup`. Primary and
  secondary keys are issued; both are in the dagster_v3 `.env` as
  `DOF_NOTICE_PRIM_API_KEY` and `DOF_NOTICE_SEC_API_KEY`. Two keys exist so one
  can be rotated while the other serves — use the primary and keep the secondary
  as the rotation spare, rather than treating them as a pair to load-balance.

- **Volume** (measured 2026-07-27, by `numHitsTotal` per `type`):

  ```
  157,055   all notices
   42,636   RESULT                                        <- a SUPERSET, not the filter
   31,097   ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT        <- award notices
    8,276   CANCELLED_OR_MISSING_CONCLUSION_OF_CONTRACT
       79   CHANGE_OF_CONCLUSION_OF_CONTRACT              <- amendments
    1,000   numHitsAccessible ceiling = min(total, 1000)
  ```

  `RESULT` was guessed earlier to be "the more robust filter"; it is not, it is a
  **wider** one that sweeps in the 8,276 cancelled notices. Use
  `ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT`. `type` is repeatable and additive —
  award + change returns 31,176 = 31,097 + 79 exactly — so amendments can be
  folded in later without changing the fetch shape.

## 2. Why this source is worth adding

Norway is currently TED-only, so every Norwegian contract below the EU
publication threshold is absent — the coverage row says so in as many words.
Doffin is the register that fills it.

It is also the cheapest of the three national sources to add, because the
identity problem that dominated the others does not exist here:

- **Sweden's UHM** needed person-vs-company classification and publishes no value.
- **Brazil's PNCP** needed a 14-digit establishment resolved to an 8-digit
  company base, with a `headquarters_cnpj` trap that half-works.
- **Doffin** publishes `organizationId` directly, and it joins `no_companies`
  with no transformation. Verified live: winner `979750730` resolves to
  KONGSBERG MARITIME AS and buyer `930068128` to TROMS FYLKESKOMMUNE.

Norway also publishes [eForms](https://github.com/anskaffelser/eforms-sdk-nor),
the same standard as TED, so the cross-source key may work the way Finland's
published `ted_number` did rather than the way Sweden's name hash did not — see
§5.

## 3. Response shape

### 3.1 Search hit

One hit carries: `id`, `type`, `allTypes`, `buyer[]`, `heading`, `description`,
`cpvCodes`, `lots[]`, `estimatedValue`, `publicationDate`, `issueDate`,
`deadline`, `status`, `receivedTenders`, `allReceivedTenders`, `locationId`,
`limitedDataFlag`, `doffinClassicUrl`.

**Winners are nested inside lots**, not at the top level:

```json
"lots": [{"heading": "...", "winner": [{"organizationId": "979750730",
                                        "name": "KONGSBERG MARITIME AS"}]}]
```

So the grain is one row per (notice, lot, winner) — the same shape as TED and
Hilma, and unlike UHM's flat awards or PNCP's one-row-per-contract.
`receivedTenders` is a bidder *count*, not the bidders: Doffin publishes who won,
not who bid, so the contract page's "participants" caveat holds here too.

Two fields read as consistently null on live award notices and should not be
relied on: `status` and `doffinClassicUrl`. `issueDate` is a full timestamp
(`2025-01-29T19:04:53Z`) while `publicationDate` is a bare date (`2025-01-31`);
they differ, and only `issueDate` is filterable.

### 3.2 Notice document (the eForms XML)

`GET /v2/download/{doffinId}` returns a UBL `ContractAwardNotice`. The part that
matters is `efac:NoticeResult`, which is where the realized money lives (§0.2)
and where winners are keyed by an id that resolves to an org number:

```
efac:NoticeResult
├── cbc:TotalAmount ................................. BT-161, notice value
├── efac:LotResult
│   ├── cbc:TenderResultCode ....................... selec-w | open-nw | ...
│   ├── efac:LotTender/cbc:ID ...................... -> TEN-nnnn
│   ├── efac:ReceivedSubmissionsStatistics .......... bid counts by type
│   └── efac:TenderLot/cbc:ID ...................... -> LOT-nnnn
├── efac:LotTender
│   ├── cbc:ID ..................................... TEN-nnnn
│   ├── cac:LegalMonetaryTotal/cbc:PayableAmount ... BT-720, THE per-winner value
│   ├── efac:TenderingParty/cbc:ID ................. -> TPA-nnnn
│   └── efac:TenderLot/cbc:ID ...................... -> LOT-nnnn
└── efac:TenderingParty/efac:Tenderer/cbc:ID ....... -> ORG-nnnn

cac:ContractingParty + efac:Organizations/efac:Organization
└── efac:Company/cac:PartyLegalEntity/cbc:CompanyID . the Norwegian org number
```

The join is `LotTender → TenderingParty → Tenderer → Organization`, which turns
BT-720 into an amount per named winner. Note `cbc:PayableAmount` sits under
`cac:LegalMonetaryTotal`, not directly under `LotTender` — a regex that misses
that layer reports "no realized value exists", which is how the first pass at
this got the wrong answer.

## 4. Ingest mode — partitioned API incremental, two stages

**Monthly partitions keyed on `issueDate`**, from **2018-01**. Publication date
is not filterable (§0.1), so issue date is the only possible partition key.
Nothing exists before 2018 and the busiest month is 378 notices against a
1,000-result ceiling, so the cap is never approached.

Per partition, roughly:

```
stage 1   search    ~300 notices / 100 per page  =   4 requests
stage 2   download  one XML per notice           = ~300 requests
```

Both stages snapshot raw to S3 before any parsing, following PNCP: ~31,400
requests for a full backfill is not something to pay twice because a parser
changed. At the measured 125 req/min the whole register is ~4.5 hours, and one
month is ~2.5 minutes.

Type filtering is applied at the API: it cuts 157,055 to 31,097, a five-fold
reduction in both stages.

### 4.1 Which value column gets what

The question §0.2 was written to answer, now that all three figures exist:

| column | source | grain | notes |
|---|---|---|---|
| `value_amount_original` | BT-720 `PayableAmount` | (notice, lot, winner) | the contract value; NULL where the notice omits it |
| `notice_value_amount_original` | BT-161 `TotalAmount` | notice | repeated across the notice's rows, as for TED |
| `estimated_value_amount_original` | search `estimatedValue` | notice | **an estimate** — a separate column, never merged into the two above |

Keeping the estimate in its own column is the whole point of the earlier
warning: it is present on ~50% of notices, absent on all of 2023-05, and where
both exist it can differ from the realized figure by 40%. A column that
sometimes holds an estimate and sometimes a realized value makes both
untrustworthy, so they stay apart and the UI can say which it is showing.

Currency is `NOK` in every sample; convert to USD in the separate FX step keyed
on `issueDate`, per the guidelines, rather than inline.

## 5. The `no_government_contracts` view

Doffin becomes a second branch beside TED, making Norway's view a union like
Sweden's and Finland's rather than the single branch it is today. Nothing else
changes: the view already exists and the country's contracts tab already reads
it.

`contract_key` should be built from whatever cross-reference Doffin publishes to
TED, if any. Finland's `ted_number` produced 2,237 matched contracts where the
buyer/date/title hash produced zero, so look for a published reference before
falling back to the hash.

**Still unresolved.** No OJS number, `efac:Publication` block or `ted.europa`
reference appears in the three award XMLs read so far — but all three were
below-threshold (`can-social` twice, and one `RegulatoryDomain` of `other`),
which would never have gone to TED. The check must be repeated on an
**above-threshold** notice before concluding none is published. Doffin's
internal DTO carries a `sentToTed` boolean (§7), so the fact is tracked
somewhere; whether it surfaces in the public XML is the open question.

Two fields found in the XML that the design did not expect, both useful:

- **`cbc:RegulatoryDomain`** settles `directive_governed` as a *published* flag
  rather than the inference this section previously assumed. It reads
  `32014L0024` (Directive 2014/24/EU) on directive-governed notices and `other`
  on nationally-regulated ones. Map it directly instead of leaving it `''`.
- **`cbc:ContractFolderID`** is a stable procurement-level UUID shared by a
  competition notice and its award notice. That is the natural within-Doffin key
  for tying an award back to the tender it resolves, independent of whatever
  cross-source key TED matching eventually uses.

## 6. Verification

- **Tests**: `tests/test_norway_doffin_*.py` — the winner projection out of
  `lots[]`, award-type filtering, org-number passthrough, export/migration
  column contract, view shape. Plus, specific to the XML stage: the
  `LotTender → TenderingParty → Tenderer → Organization` walk that attaches
  BT-720 to a named winner, and that `cbc:PayableAmount` is read from under
  `cac:LegalMonetaryTotal` rather than directly under `LotTender`.
- **Live**: one partition → check counts against `numHitsTotal` for that slice →
  confirm winner org numbers resolve against `no_companies` → confirm the
  Norwegian contracts tab stops being empty.
- **The check that a date filter is real**: assert `numHitsTotal` differs
  between two partitions. A partition whose total equals the unfiltered 31,097
  means the filter was dropped (§7), and that must fail the run rather than load
  the whole register 100 times over.

## 7. Issues found during investigation

- **Unrecognised query parameters are silently ignored.** Three different
  guessed date parameters all returned HTTP 200 and the unfiltered 157,050. This
  is the most dangerous behaviour found: a filter that does not work looks
  exactly like one that does, and a partitioned backfill built on a guessed
  parameter would fetch the same full set for every partition while appearing
  correct. Read the parameter names from the API definition.

  Refined 2026-07-27: this applies to parameter *names* only. Invalid enum
  *values* are rejected with a 404 (§0.4), so `type` and `sortBy` fail loudly
  while a misspelt date parameter fails silently. The test that distinguishes a
  working date filter from an ignored one is that **`numHitsTotal` moves** —
  never that the request succeeded.
- **The published API definition is partly stale.** The `sortBy` enum in the
  schema document lists `ISSUE_DATE_ASC`/`DESC`, which 404; the operation
  description lists `PUBLICATION_DATE_ASC`/`DESC`, which work. The definition is
  still the right source for parameter *names* — it was right about
  `issueDateFrom` — but values need a live check.
- **A richer notice DTO exists in the definition but not on the public path.**
  `PagedNoticeDto` (lot-grain `estimatedValue`, `awardResult`, `contract`,
  `receivedSubmissions`, and a `continuationToken` that would sidestep the 1,000
  cap) is fully specified in the schema and referenced by a
  `V2NoticesWithDatesGetRequest`, but `/v2/notices-with-dates`, `/v2/noticesWithDates`
  and `/v2/notices` all 404 and the operation is not listed. It is presumably
  the internal `webclient/api/v2` API that doffin.no itself calls. Worth
  re-checking if the public API is ever extended, since it would replace the
  per-notice download entirely.
- **No detail endpoint was found** by guessing `/public/v2/notices/{id}` and
  variants — all 404. The search response appears to be the complete record,
  which is convenient, but confirm against the API definition rather than
  assuming nothing more is available.
- **The 1,000-result cap is not documented in the response** beyond
  `numHitsAccessible`, and it did not move when a filter cut the total from
  157,050 to 31,095.
