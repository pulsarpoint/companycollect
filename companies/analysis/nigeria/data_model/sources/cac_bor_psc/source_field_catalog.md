# CAC Beneficial Ownership Register (PSC) Field Catalog

## Source Summary

- Country: Nigeria
- Source type: beneficial_ownership
- Organization: Corporate Affairs Commission (CAC)
- URL: https://borapp.cac.gov.ng/api (search: /bor-search/get_psc)
- License: public register, token-gated API
- Access: **public via browser; API token-gated**
- Freshness: live
- Record shape: JSON PSC search results (token-gated)
- Primary keys: rc_number
- Join keys: rc_number, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | companyName | Company name | string | legal_name |  | join via RC |
| rc_number | rcNumber | Company RC number | string | identifier |  | join key |
| psc_name | pscName | Beneficial owner name | string | person |  | PERSONAL DATA — redact |
| nature_of_control | natureOfControl | Nature of control | string | ownership |  | |
| shareholding_percent | shareholdingPercent | % held | decimal | ownership |  | |
| nationality | nationality | PSC nationality | string | person |  | PERSONAL DATA |

## Interpretation Notes

- **`bor.cac.gov.ng`** — the CAC **Beneficial Ownership Register** ("Persons With
  Significant Control"), a public PSC register. It is a React SPA whose API base is
  **`borapp.cac.gov.ng/api`**, with search at **`/bor-search/get_psc`** (POST) and
  details at `/bor-search/get_psc_details`.
- **Access (verified)**: the API returns **401 Unauthorized** without an access
  token; `get_psc` is **POST** (405 on GET). It is **public via the browser** but
  **token-gated** for automation. **Not bypassed** — fields above are documented from
  the SPA model; **no live values captured**.
- **SECURITY / PRIVACY NOTE**: a token-less POST to `/auth/access-token` (the SPA's
  "profile_user_data" call) returned **HTTP 200 with an individual user's personal
  profile** (a broken-access-control misconfiguration). This was **not used, not
  stored, and not pursued**; it is recorded only as a data-protection concern. Do not
  call that endpoint.
- **Join**: keyed on the **RC number** (and company name) to the CAC register.
- **Personal data**: PSC names / nationality are personal data under the **NDPA 2023**
  — **redact** in any committed output.
- Implementation is **blocked on authentication** (token) and constrained by privacy;
  planning-only.
