# Norway Doffin (government contracts) design doc

> Follows `docs/data-source-guidelines.md`; deviations are called out below.
> Status: **design only, nothing built.** Section 0 gates the build.

## 0. Open questions that gate the build

Everything else here was measured against the live API on 2026-07-27. These were
not, and the first one blocks the ingest entirely.

1. **What are the date filter parameter names?** This is the blocker. A search
   returns at most 1,000 results (§1) against 31,095 award notices, so the only
   way to read them all is to slice by date until each slice is under the cap —
   and no slicing is possible without knowing the parameter.

   **The API silently ignores unrecognised parameters.** `publishedFrom`,
   `fromDate` and `publicationFromDate` were each accepted with HTTP 200 and
   each returned the unfiltered 157,050. A wrong guess therefore does not fail,
   it returns the whole set looking like a filtered one — so this must be read
   from the portal's API definition, never inferred from a response that looks
   plausible. See `https://dof-notices-prod-api.developer.azure-api.net/apis`.

2. **Is there a realized amount, and at what grain?** `estimatedValue` is an
   *estimate* at notice level, weaker on both counts than what the other
   registers publish, and it must not be presented as a contract value.

   Do not assume this is Hilma's situation -- that comparison was wrong when
   first written here. Hilma publishes `lots_value_amount_original`, a
   **realized** value at **lot** grain, which is one winner's amount wherever a
   lot has a single winner (4,316 of Finland's lots). What Hilma lacked was a
   per-*winner* figure, not a real one.

   So this is two questions. Is there any realized value at all, or only the
   estimate? And does anything sit at lot grain, given winners are already
   nested per lot in `lots[]`? Search inside `lots[]` for a value field before
   concluding there is none -- Hilma's better field sat beside the one first
   used, and was missed.

   If only `estimatedValue` exists it belongs in neither `value_amount_*` nor
   `notice_value_*` as those are defined: both hold realized figures. An
   estimate needs its own column or none, because a column mixing estimates
   with realized values makes both untrustworthy.

3. **Does `numHitsAccessible` cap results or pages?** It read 1,000 on every
   query regardless of filter, including one whose total was 31,095. Whether
   that is a hard result ceiling or a pagination depth limit decides whether
   date slicing alone suffices.

4. **Rate limits.** Unknown. PNCP's only became visible by tripping it; do not
   discover Doffin's the same way during a backfill.

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
  | notice search | `GET api.doffin.no/public/v2/search` | JSON | 157,050 notices | continuous | **yes** |

  Auth is an Azure API Management subscription key sent as
  `Ocp-Apim-Subscription-Key`. Registration:
  `https://dof-notices-prod-api.developer.azure-api.net/signup`. Primary and
  secondary keys are issued; both are in the dagster_v3 `.env` as
  `DOF_NOTICE_PRIM_API_KEY` and `DOF_NOTICE_SEC_API_KEY`. Two keys exist so one
  can be rotated while the other serves — use the primary and keep the secondary
  as the rotation spare, rather than treating them as a pair to load-balance.

- **Volume** (measured 2026-07-27):

  ```
  157,050   all notices
   31,095   ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT   <- award notices
    1,000   numHitsAccessible, on every query        <- the constraint
  ```

- **Notice types seen** in a 100-notice sample: `ANNOUNCEMENT_OF_COMPETITION`
  (53), `ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT` (32), `ADVISORY_NOTICE` (9),
  `CANCELLED_OR_MISSING_CONCLUSION_OF_CONTRACT` (4), `ANNOUNCEMENT_OF_INTENT`
  (2). Only the award type carries winners. `allTypes` also tags those notices
  `RESULT`, which may be the more robust filter.

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

## 4. Ingest mode — pending §0.1

Cannot be settled until date filtering is understood. The shape will be a
partitioned API incremental keyed on publication date, with granularity chosen
so each partition stays under the 1,000 cap. At 31,095 award notices over the
register's life, monthly partitions would average well under the cap, but the
recent months must be checked rather than assumed.

Type filtering works and should be applied at the API rather than after: it cuts
157,050 to 31,095, which is a five-fold reduction in pages against an unknown
rate limit.

## 5. The `no_government_contracts` view

Doffin becomes a second branch beside TED, making Norway's view a union like
Sweden's and Finland's rather than the single branch it is today. Nothing else
changes: the view already exists and the country's contracts tab already reads
it.

`contract_key` should be built from whatever cross-reference Doffin publishes to
TED, if any. Finland's `ted_number` produced 2,237 matched contracts where the
buyer/date/title hash produced zero, so look for a published reference before
falling back to the hash.

`directive_governed` is meaningful here, unlike Brazil: Norway is in the EEA and
follows the EU procurement directives. A Doffin notice that also went to TED is
above threshold; one that did not may be below it. That is an inference rather
than a published flag, so it should be `''` unless Doffin states it.

## 6. Verification

- **Tests**: `tests/test_norway_doffin_*.py` — the winner projection out of
  `lots[]`, award-type filtering, org-number passthrough, export/migration
  column contract, view shape.
- **Live**: one partition → check counts against `numHitsTotal` for that slice →
  confirm winner org numbers resolve against `no_companies` → confirm the
  Norwegian contracts tab stops being empty.

## 7. Issues found during investigation

- **Unrecognised query parameters are silently ignored.** Three different
  guessed date parameters all returned HTTP 200 and the unfiltered 157,050. This
  is the most dangerous behaviour found: a filter that does not work looks
  exactly like one that does, and a partitioned backfill built on a guessed
  parameter would fetch the same full set for every partition while appearing
  correct. Read the parameter names from the API definition.
- **No detail endpoint was found** by guessing `/public/v2/notices/{id}` and
  variants — all 404. The search response appears to be the complete record,
  which is convenient, but confirm against the API definition rather than
  assuming nothing more is available.
- **The 1,000-result cap is not documented in the response** beyond
  `numHitsAccessible`, and it did not move when a filter cut the total from
  157,050 to 31,095.
