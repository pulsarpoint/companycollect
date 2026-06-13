# Company Data Analysis For Norway

## Summary

Norway supports an exceptionally rich, fully open company profile built entirely from one official
publisher — the **Brønnøysund Register Centre (Brreg)** — under **NLOD 2.0**, with no
authentication. Four complementary sources combine on a single key (`organisasjonsnummer`) into a
profile covering: legal identity, status, activity (NACE), addresses, contact, employees, corporate
group/parent, **establishments/sites**, **officers (board/CEO/auditor)**, and **full annual-accounts
financials** (income statement + balance sheet). The only notable gaps are beneficial ownership
(not open) and deep multi-year financial history (open API is shallow).

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| brregenhet | Enhetsregisteret — entities | recommended | public, no auth | NLOD 2.0 | Base record / spine |
| brregunderenhet | Enhetsregisteret — sub-entities | recommended | public, no auth | NLOD 2.0 | Establishments / sites |
| brregroller | Enhetsregisteret — roles | recommended | public, no auth (PII) | NLOD 2.0 | Officers (board/CEO/auditor) |
| brregregnskap | Regnskapsregisteret — accounts | recommended | public, no auth | NLOD 2.0 | Financial statements |

(Catalog-only / excluded: data.norge.no = catalog reference; Beneficial Ownership Register =
not open, excluded.)

## What Each Source Contributes

- **brregenhet (entities)** — the spine. Org number, legal name, legal form, up to three NACE
  codes, business + mailing addresses, employees, website/phone, share capital, status flags
  (bankruptcy/liquidation), register memberships, group flag, and the `sisteInnsendteAarsregnskap`
  signal used to trigger financial refresh. 1,164,396 active entities; bulk JSON ~197 MB / CSV
  ~154 MB; daily delta feed.
- **brregunderenhet (sub-entities)** — establishments/operating sites (842,538), each with its own
  org number, activity code, and physical `beliggenhetsadresse`. Joins to the parent via
  `overordnetEnhet`. Gives site-level coverage.
- **brregroller (roles)** — officers: general manager (DAGL), board (STYR: chair/member/deputy),
  auditor (REVI). Each role-holder is either a natural **person** (PII) or a **company** (`enhet`,
  e.g. audit firm — a join key back to entities). Per-org-number lookup. **Contains personal data
  (names + birth date) → handle under GDPR.**
- **brregregnskap (financials)** — the financial data requested. Per org number, an array of annual
  accounts (one per period × SELSKAP/KONSERN) with operating revenue, operating result, net
  financial items, pre-tax and net result, total/current/fixed assets, equity, total/current/
  long-term debt, currency, audit and small-enterprise flags. ~80% of accounting-liable companies.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`, built from Equinor ASA 923609016) models a
Norway-specific object with sections: `registration`, `legal_identity`, `status`, `activity`,
`addresses`, `contact`, `employment`, `group`, `establishments[]`, `officers[]`,
`financial_statements[]`, `filing_signals`, and `source_provenance[]`. Repeatable concepts
(NACE codes, establishments, officers, yearly financials) are arrays. Every major section carries
`x-source` provenance. Status is a derived enum plus the underlying Brreg booleans. Personal data in
`officers[]` is minimized (birth year only) and flagged.

## Join And Precedence Rules

- **Single join key**: `organisasjonsnummer`. All sources attach to the `brregenhet` spine.
  `overordnetEnhet` joins establishments and group children back to the parent.
- **No cross-source conflicts** — one publisher (Brreg), complementary datasets. Where
  Regnskapsregisteret has both SELSKAP and KONSERN accounts for a year, prefer SELSKAP for the
  entity's own figures, keep KONSERN as the consolidated view.
- **Build order**: entities → establishments → officers → financials. Gate financial fetches on
  `sisteInnsendteAarsregnskap` changing (avoids needless per-orgnr calls).
- **Freshness**: entities/sub-entities/roles daily (delta feeds); financials ~weekly / annual cycle.

## Missing Or Restricted Data

- **Beneficial ownership** (reelle rettighetshavere): not open as bulk/API → excluded.
- **National ID number (fødselsnummer)** of officers: only via authenticated autorisert-api
  (Maskinporten) → excluded; the open roles endpoint gives names + birth date only.
- **Deep financial history**: the open Regnskapsregisteret API is labelled "temporary/research" and
  carries shallow history (recent year[s]). Full multi-year history + scanned image copies are
  behind the **paid Subscription Service** (planning-only, not ingested).
- **Full group structure**: only `erIKonsern` + `overordnetEnhet`; the complete group tree is not
  enumerated.
- **Personal-data caveat**: officer names/birth dates are open under NLOD but still require a GDPR
  lawful basis and retention policy before persisting beyond the raw zone.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Key points for a future cross-country mapper: Norway uses
one identifier for company id / registration number / (with `NO…MVA`) VAT id — expect no separate
tax id; financial figures carry a **currency that is not always NOK**; Norway offers
establishment-level data many countries lack; `owners` is `not_available_in_open_sources`.
