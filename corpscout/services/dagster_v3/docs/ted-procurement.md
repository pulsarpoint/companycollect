# TED procurement — cross-country reference

`ted_procurement` is a **country-agnostic** source: one pipeline that ingests
EU-threshold public-contract award notices for *any* EU/EEA country from TED
(Tenders Electronic Daily, the EU's official procurement journal). Countries are
configuration, not code — Finland is simply the first one turned on.

This is the shared reference. Per-source implementation *decisions* live in the
module design doc: `src/dagster_v3/defs/ted_procurement/docs/ted_procurement-design.md`.
Discovery trail (API probes, sample notices, licence): `companies/analysis/finland/`.

## Why this is a separate, shared source

Every EU member state's above-threshold contracts are published to TED in a
uniform **eForms** schema, keyed by the winner's **national registration number**
(`cbc:CompanyID schemeID="002"`). That means one parser and one set of tables serve
all countries — the winner→company join is the same shape everywhere, differing only
in how each country's national id is normalised. National *below*-threshold contracts
are **not** on TED; those need a per-country portal source (e.g. `finland_hilma`).

## What it produces

Two ClickHouse tables (migration `000148`), each `country_iso2`-tagged so every
country lands in the same place:

| Table | Grain | Join surface |
|---|---|---|
| `corpscout.ted_notices` | 1 row per `publication_number` | buyer (`buyer_national_id`) |
| `corpscout.ted_notice_winners` | 1 row per `(publication_number, lot_id, tender_id, winner_ordinal)` | **`winner_national_id`** (ORDER BY key) |

Amounts carry the standard `*_amount_original` + `*_amount_usd` + currency + fx trio,
converted per-amount on the publication date. `winner_national_id` is the normalised
national id (raw form kept in `winner_national_id_raw`); `winner_country` /
`place_country` distinguish the winner's home country from where the work is performed.

## Architecture (country-free core + config)

```
tables.COUNTRIES  (config: place_code -> country_iso2)
        │
ted_monthly_snapshot   search API per country + per-notice eForms XML -> S3
        │              (monthly publication-date partitions, 2024-01+)
ted_monthly_duckdb     lxml parse -> partition DuckDB
        │              (organizations, winner links, national-id normalization)
ted_publish_clickhouse union ALL partitions -> USD -> ted_notices + ted_notice_winners
```

- **Source**: keyless TED v3 API — `POST api.ted.europa.eu/v3/notices/search`
  (max 250/page) for listings, `ted.europa.eu/en/notice/{n}/xml` for eForms.
- **Core is country-free**: `client.py`, `parser.py`, `publish.py` never name a
  country. The parser resolves the eForms winner chain
  (`LotResult → LotTender → TenderingParty → Company`), handling multi-lot notices,
  multi-supplier framework awards (several winners per lot), and consortia — tested
  against real FIN *and* SWE notices to keep that guarantee honest.
- **eForms only** (mandatory on TED late 2023) → partitions start `2024-01`. Earlier
  legacy-schema notices are out of scope; a national portal source covers older history.

## Adding a country

1. **Add one config row** in `defs/ted_procurement/tables.py`:
   ```python
   COUNTRIES = (
       TedCountry(place_code="FIN", country_iso2="FI"),
       TedCountry(place_code="SWE", country_iso2="SE"),   # <- new
   )
   ```
   `place_code` is TED's ISO-3166 alpha-3 place-of-performance code; `country_iso2`
   is our register key.
2. **Optionally add a national-id normalisation rule** in `NATIONAL_ID_NORMALIZATION`
   (a `(regex, replacement)` pair) if the country's eForms `CompanyID` needs cleaning
   to match the register key — e.g. Finland strips the `FI…` VAT form to the Y-tunnus.
   No rule → ids pass through verbatim (Swedish org numbers already match, so none is
   needed).
3. **Re-materialise the history.** A monthly partition only searched the countries
   configured *when it ran*, so after adding a country **backfill all partitions from
   the UI** (`ted_procurement_job`, 2024-01 → current) to pull the new country's
   notices; existing XML objects are reused, so re-runs only fetch the new country.
   Then run `ted_publish_job`.
4. **Surface it (optional)** — the backoffice `publicContractsQuery` for that country
   adds a `ted_notice_winners` branch (see `finland` in `backoffice/app/lib/countries.ts`
   for the pattern). Everything else — parser, tables, USD, schedule — is unchanged.

Nothing about steps 1–3 is Finland-specific; that is the whole point of the package.

## Operations

- **Schedule**: `ted_procurement_schedule` runs `ted_procurement_job` monthly on the
  3rd at 05:35 (Europe/Belgrade), refreshing the just-closed month; `end_offset=1`
  keeps the current month materialisable. Publish (`ted_publish_job`) runs after.
- **Backfills**: launch from the Dagster UI — `BackfillPolicy.multi_run(1)` throttles
  to one small run per partition (no event-log connection storm).
- **Rate limits**: `ted.europa.eu` sporadically 429s the XML endpoint (~13/578 even
  throttled) — the client honours `Retry-After` with backoff up to 6 attempts and a
  0.2 s inter-download throttle. Retries survive both returned-429s and the dlt
  session's *raised* 429s.
- **Idempotent & resumable**: XML is cached in S3 (`source-ted-procurement`) and each
  parsed month is a local partition DuckDB, so an interrupted backfill resumes without
  re-fetching, and publish is an atomic stage + replace.

## Configured countries & coverage

| Country | `place_code` | Range | Notices | Winner rows | Distinct companies joining register |
|---|---|---|---:|---:|---:|
| Finland | FIN | 2024-01 → 2026-06 | 12,294 | 44,816 | 6,990 (`fi_companies`) |

100% of winner rows carry a national id. Finland is also served by
`finland_hilma` for national below-threshold contracts; the two are unioned and
de-duplicated (on Hilma's `ted_number` reference) in the backoffice public-contracts
section. Other countries would rely on TED alone until/unless a national portal
source is added.
