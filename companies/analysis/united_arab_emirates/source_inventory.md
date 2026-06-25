# Source inventory — United Arab Emirates

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| National Economic Register (NER) | Unified company search | Ministry of Economy | Login-gated | html | no | blocked_by_authentication |
| Emirate DEDs (Dubai DET, ADDED, …) | Trade-license registries | Emirate DEDs | Per-emirate WAF/login | html | no | blocked_by_authentication |
| Free-zone registers (DIFC, ADGM) | Free-zone public registers | DIFC / ADGM registrars | Browser; WAF/rate-limited | html | no | blocked_by_authentication |
| DFM & ADX | Listed financials | DFM / ADX | Browser; WAF/auth-gated | html, json, pdf | yes (listed) | blocked_by_authentication |
| bayanat.ae / data.gov.ae | Open-data portals | Gov of UAE | Unreachable | — | no | unavailable |

## Identifiers

- **Trade / Commercial License number** — issued per **emirate DED** or **free zone**.
- **TRN — Tax Registration Number** — 15-digit (FTA; VAT + corporate tax).
- **Economic register number** — under the NER (national unified).
- **Free-zone registration number** — DIFC / ADGM / DMCC, etc.

## Key facts

- **No single national company register** — registration is split across emirate DEDs,
  free-zone registrars (DIFC/ADGM), and the federal NER.
- **All gated** from this environment: NER login-only; emirate DEDs WAF/login (Invest
  in Dubai 403); DIFC public register 429; ADGM search app 403; DFM SPA + ADX 403;
  open-data portals unreachable.
- **No open bulk register; no open programmatic financials** — listed financials are
  public via the browser (DFM/ADX) but WAF/auth-gated for automation.
- Currency **AED**; Arabic + English. Owners/managers are personal data (PDPL,
  Federal Decree-Law 45/2021) → redact.
