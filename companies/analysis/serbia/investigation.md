# Serbia — Company and Representative Data Investigation

Last verified: 2026-08-24
Country: Serbia (RS)
Languages searched: English and Serbian (Latin and Cyrillic)

## Conclusion

The user-supplied endpoint is an official, working APR open-data endpoint and is
the best source for a monthly company backbone. It is not sufficient for a
company-representative data product because the payload contains no person,
officer, representative, member, procurist, or board fields.

The recommended solution is hybrid:

- **Open APR JSON** for company identity, status, legal form, municipality,
  incorporation date and activity code.
- **Paid APR delivery/web service** for legal representatives and related
  status-register data.

## 1. APR Companies Open Data API — recommended for company core

- Endpoint: `https://openapi.apr.gov.rs/api/opendata/companies`
- Publisher: Agencija za privredne registre (APR)
- Access: public GET, no authentication observed
- Format: one full JSON snapshot; no pagination
- Live snapshot date: `2026-07-31`
- Live record count: `133634`
- Download size: `57,673,691` bytes
- Primary key: the `Podaci` map key (`matični broj`)
- Update cadence: APR's catalog description says monthly
- License: Serbian Open Data License (`sodl`; SODL 1.0)

Observed envelope:

```json
{
  "DatumPreseka": "2026-07-31",
  "Podaci": {
    "17246771": {
      "PoslovnoIme": "...",
      "SifraOpstine": "70181",
      "NazivOpstine": "НОВИ БЕОГРАД",
      "NazivStatus": "Активан",
      "DatumOsnivanja": "2000-03-30",
      "NazivPravneForme": "Друштво са ограниченом одговорношћу",
      "SifraDelatnosti": "4719"
    }
  }
}
```

A complete enumeration of nested object keys confirmed that there are exactly
seven fields. Searches for representative/director/procurist terminology in the
schema found no representative fields.

### Important limitations

- No legal or other representatives
- No directors, procurists or board members
- No founders/members/shareholders
- No PIB/VAT number
- No full street address; only municipality code/name
- No entrepreneurs (`preduzetnici`)
- No deleted companies in the open feed description

`HEAD` returned HTTP 405 with `Allow: GET, OPTIONS`; collectors should perform a
GET and avoid using HEAD as a health check.

## 2. APR one-off electronic data delivery — best backfill option

APR's official "Statusni i drugi poslovni podaci" service accepts paid requests
for individual company records and selected data sets.

The 2026 schedule defines these company sets:

| Set | Relevant contents | Fee per registered subject |
|---|---|---:|
| SP2 | Expanded company base, including PIB, legal form, activity and capital | 25 RSD |
| SP3 | Seat/address/contact groups and **legal representatives** | 5 RSD |
| SP4 | **Other representatives**, boards, procurists, group procura | 5 RSD |
| SP5 | Members/founders, ownership/share data | 5 RSD |
| SP6 | Branches, branch representatives/procurists, notes and notices | 5 RSD |

SP3-SP6 can only be ordered together with SP2. Therefore:

- legal representatives require **SP2 + SP3 = 30 RSD/entity**;
- legal plus other representatives/procurists require
  **SP2 + SP3 + SP4 = 35 RSD/entity**.

The standard electronic format is XLS/XLSX; MDB is available by special
request. APR accepts requests by email at `apr-podaci@apr.gov.rs`.

## 3. APR automated data-delivery web service — best incremental option

APR states that its contracted web service covers all publicly available data
in its status registers, including companies and entrepreneurs. It exposes two
basic operations:

1. retrieve all changes in a requested time period, commonly daily;
2. retrieve selected data groups for a specified `matični broj`.

State bodies use it without a fee; banks and other businesses pay a prescribed
fee. A standard contract is required. Public pages do not expose the exact
protocol, schema, authentication method, rate limits or representative field
structure; request technical documentation and a sample response from APR.

## 4. APR public company search — manual only

APR's public search is suitable for a person manually verifying an individual
company. It is not an ingestion source:

- APR's search page explicitly says unauthorized downloading with applications,
  scripts or other automated tools will be blocked.
- APR's terms prohibit automated tools against search results.
- The current search UI presents reCAPTCHA.

No attempt was made to bypass the CAPTCHA or automate the public search.

## 5. Other APR open data

- Financial statements:
  `https://openapi.apr.gov.rs/api/opendata/companies/financial-statements`
- Associations/foundations:
  `https://openapi.apr.gov.rs/api/opendata/ngo`

These can enrich company/legal-entity profiles but do not solve the company
representative requirement.

## Recommended implementation

Use `matični broj` as the company key. Download the open company feed monthly,
validate the snapshot date and field set, preserve the raw JSON and content hash,
then upsert the company projection. Obtain a representative backfill through
SP2+SP3 (and SP4 where required), then apply daily web-service changes into a
versioned `company_representative` relationship table.

Before implementation, ask APR for:

- WSDL/OpenAPI or equivalent protocol documentation;
- authentication and sandbox details;
- exact schemas for SP3 and SP4;
- stable representative/relationship identifiers;
- deletion, replacement and effective-date semantics;
- fee calculation, quotas and rate limits;
- permitted retention and redistribution;
- which personal identifiers are suppressed from commercial users.
