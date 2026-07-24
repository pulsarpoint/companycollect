# Sweden company listings

## Purpose

`corpscout.se_company_listings` is the Sweden-owned listing evidence table.
Its grain is one current `(company_id, isin, mic)` relationship. Other
countries will publish their own physical table with the same contract so a
country refresh never rebuilds every country's listings.

The main company list will later derive `is_publicly_traded` from the
existence of at least one current row. Counts and market arrays do not belong
in the main list table.

## Resolution path

```text
FIRDS current Swedish equity listing
  (ISIN, MIC, CFI, issuer LEI)
        |
        v
GLEIF LEI record
  registered_as -> digits-only 10-character organisation number
        |
        v
se_companies.company_id
```

The first release accepts only the FIRDS Sweden MIC scope declared in
`esma_firds/listing_scopes.py` and CFI category `E`. Identity resolution is
exact. Company-name, ticker, and fuzzy matching are deliberately excluded.

EODHD is left-joined on the exact `(ISIN, MIC)` pair. It enriches a resolved
FIRDS row with a ticker, instrument type, and vendor delisting state, but it
cannot create a listing or override FIRDS current status. A FIRDS-current row
that EODHD marks delisted is retained with `status_conflict=1`.

## Publication

The migration owns the target schema. The Dagster asset builds a
UUID-suffixed schema copy, validates the staged grain and required
identifiers, and publishes with `EXCHANGE TABLES`. Empty results, expanded
candidate grain, duplicate `(company_id, isin, mic)` keys, and invalid
identity rows fail before the exchange.

This first implementation is a full Sweden rebuild. It intentionally does
not update `companies_all` or create a global physical listing table.
