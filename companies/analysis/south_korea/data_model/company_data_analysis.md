# Company Data Analysis For South Korea

## Summary

South Korea has a **strong open financial + identity API (free key)** for the
disclosure-obligated universe, **free-key tax-status lookups**, and a **paid full
register** for the unlisted long tail. The **OpenDART API** (Financial Supervisory
Service) is the anchor: with a free `crtfc_key` it returns the bulk DART entity
list (`corpCode.xml`), company identity (`company.json`, including **both** the
13-digit corporate registration number and the 10-digit business registration
number), and **XBRL financial statements** (`fnlttSinglAcntAll`) in KRW. Coverage
= all **listed** + **external-audit** companies.

NTS's business-status API (free data.go.kr key) adds operating status. The court
commercial registry (IROS) holds the **unlisted** long tail plus exact legal form,
capital, and directors, but is **fee-based**. Korea has **VAT**, but the VAT number
**is** the business registration number — no separate VAT id. The example is
schematic (key-gated APIs).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| opendart_api | OpenDART API (FSS/DART) | blocked_authentication | free API key | public disclosure | Authoritative identity + financials |
| nts_business_status | NTS business-status API | blocked_authentication | free data.go.kr key | KOGL | Tax-registration status |
| iros_court_register | IROS Supreme Court registry | blocked_payment | paid per-document | restricted | Unlisted register, legal form, capital, directors |

## What Each Source Contributes

- **opendart_api** — the authoritative open layer: corpCode.xml (entity list),
  company.json (names KO/EN, market class, **jurir_no** 13-digit corp reg no,
  **bizr_no** 10-digit business reg no = tax id, CEO, establishment date, address,
  KSIC industry), and fnlttSinglAcnt(All) (full XBRL financials, KRW). Free key;
  schema confirmed from the official API guide (HTTP-rejected without a key).
- **nts_business_status** — operating status (active/suspended/closed) and VAT
  taxpayer type by business registration number. Free data.go.kr key.
- **iros_court_register** — the only complete source of **unlisted companies**,
  exact **legal form**, **capital**, and the full **director list**. Fee-based;
  directors are personal data.

## Proposed Country Company Profile

A single object keyed on `registration.corp_registration_number` (with the
business registration number, DART corp_code, and stock_code):

- `registration` — corp reg no, business reg no, DART corp_code, stock_code.
- `tax_identifiers` — tax_id = vat_id = business registration number.
- `legal_identity` — name KO/EN, market class, exact legal form (paid).
- `status` — business status / tax type / closure date (NTS).
- `incorporation` — establishment date.
- `activity` — KSIC industry code.
- `registered_location` — address, homepage.
- `capital` — registered capital (court registry, paid).
- `financial_statements[]` — OpenDART XBRL (KRW).
- `officers[]` — CEO (DART) / directors (court registry); personal data (PIPA).
- `source_provenance[]`.

## Join And Precedence Rules

- **Join keys**: DART corp_code within OpenDART; business registration number ↔
  NTS; corporate registration number ↔ court registry.
- **Precedence**: OpenDART (identity + financials) > NTS (operating status) > court
  registry (legal form, capital, directors, unlisted tail).
- **No separate VAT id** — it is the business registration number.

## Missing Or Restricted Data

- **Unlisted micro/SME companies** and **non-DART financials** — court registry
  (paid) only / not open.
- **Exact legal form, capital, full director list** — court registry (paid).
- **Shareholders/owners** — not openly published (partial via DART major-holder
  filings).
- **CEO/directors** — personal data (PIPA), redact.

## Common Mapper Notes

- Map `company_id`/`registration_number` to the corporate registration number (or
  DART corp_code); map `tax_id` and `vat_id` both to the business registration
  number.
- Map `financials` from OpenDART (free key, KRW, DART-registered only).
- Redact CEO/director personal data (PIPA) in any committed output.
