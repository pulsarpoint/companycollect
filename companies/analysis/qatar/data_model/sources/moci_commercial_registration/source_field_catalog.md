# MoCI Commercial Registration Field Catalog

## Source Summary

- Country: Qatar
- Source type: official_registry
- Organization: Ministry of Commerce and Industry (MoCI)
- URL: https://www.moci.gov.qa/en/
- License: restricted
- Access: **lookup-only / auth-gated** (no open bulk/API)
- Freshness: live
- Record shape: per-CR lookup (planning-only)
- Primary keys: cr_number
- Join keys: cr_number, establishment_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cr_number | Commercial Registration No. | Onshore company id | string | identifier |  | primary onshore key |
| establishment_name | Trade / Establishment Name | Registered name | string | legal_name |  | Arabic primary |
| legal_form | Legal Form | Legal form | string | legal_form |  | W.L.L./Q.P.S.C./etc. |
| activities | Activities | Licensed activities | array | activity |  | ISIC-based |
| status | Status | CR status | string | status |  | active/expired/cancelled |
| capital | Capital | Registered capital | decimal | financial |  | QAR |
| owners | Owners / Partners | Owners/partners | array | ownership |  | **PERSONAL DATA — redact** |
| manager | Manager | Authorized manager | string | person |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- **MoCI Commercial Registration** is the **onshore Qatari companies registry** — the bulk
  of the economy, keyed on the **Commercial Registration (CR) number**. This is the
  authoritative identifier source for non-financial-centre companies.
- **Access**: the MoCI main site is reachable, but the e-service paths returned **404**
  (portal restructured) and the Single Window hosts (`businessinqatar.gov.qa`,
  `business.gov.qa`) **did not resolve**. CR verification is a **per-CR lookup** (often
  Arabic, commonly behind the national portal/authentication). **No open bulk file or API**
  was found — all fields here are **planning-only**, documented from public knowledge; **no
  per-company values were captured or reproduced**.
- **Language**: Arabic is primary, English secondary. **Currency** QAR. Dates Gregorian.
- **Personal data**: owners/partners and manager are personal data under Law No. 13 of
  2016 — redact natural persons.
- No `sample_record.json`: restricted/auth-gated source, nothing captured.
