# Croatia Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Croatia's defining trait: a single
clean key (OIB) joins an open API register to open structured financials** — both under the Otvorena dozvola,
behind a free registration.

## Identity / legal / activity / location / persons (register)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.oib | sudski_registar | oib | **PK** | continuous | Otvorena dozvola / free key | = VAT root |
| registration.mbs | sudski_registar | mbs | join | continuous | open | court register no |
| legal_identity.name | sudski_registar | tvrtka/naziv | — | continuous | open | |
| legal_identity.legal_form | sudski_registar | pravni_oblik | — | continuous | open | d.o.o./j.d.o.o./d.d. |
| status.* | sudski_registar | status | — | continuous | open | aktivan/likvidacija/brisan |
| activity.nkd_codes | sudski_registar | predmet_poslovanja | — | continuous | open | NKD where coded |
| registered_location.* | sudski_registar | adresa/sjediste | — | continuous | open | derive županija |
| capital.* | sudski_registar | temeljni_kapital | — | continuous | open | EUR since 2023 |
| officers_and_owners[] | sudski_registar | osobe | oib | continuous | open · **PII** | **members + management both open** |
| beneficial_owners[] | rsv_beneficial_ownership | stvarni vlasnici | oib | continuous | **restricted** | planning-only; sensitive PII |

## Financial statements (open, structured)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] | fina_rgfi | bilanca + RDG (CSV) | **oib** | annual | **open** (Otvorena dozvola; free login) | clean join; AOP positions |

### Financial precedence
- **Single open source**: `fina_rgfi` (Otvorena dozvola CSV). No paid tier needed for micro/small (the open
  set); fuller/large-company data may need the **paid FINA product** — note the coverage caveat. Dedupe on
  `oib + fiscal_year`; revenue/employees nullable for mikro/mali; currency EUR since 2023 (HRK earlier).

## Discovery

| Profile aspect | Source | Notes |
|---|---|---|
| dataset discovery + license confirmation | data_gov_hr | CKAN package_show confirmed both datasets = Otvorena dozvola; resources point to the gated portals |

## Join & precedence summary

- **Single clean key**: the **OIB** (= VAT root) keys the Sudski registar, the FINA RGFI financials, and the
  RSV beneficial ownership — **no fuzzy matching** (MBS also available for the court register).
- **Authority**: Sudski registar authoritative for identity/status/activity/capital/persons; FINA RGFI for
  financials; RSV (restricted) for beneficial ownership.
- **Build order**: Sudski registar API (spine) → FINA RGFI (join on OIB) → (RSV only with lawful access).
  Freshness: register continuous, financials annual.
- **Normalization**: Croatian; NKD where coded; EUR/HRK boundary (2023); both core sources need free registration.

## Missing / restricted data — minimal

- Almost nothing is missing: identity, **financials**, activity, capital, **officers + owners** are all open
  and clean-joined.
- **Beneficial ownership (RSV)**: restricted (planning-only) — but **members/owners + management are OPEN** in
  the register (osobe).
- **Access**: both core sources need a **free registration/account** (sudreg key; FINA login).
- **Financials nullability**: mikro/mali file abbreviated forms; large-company full data may need the paid FINA product.
- **PII**: osobe + beneficial owners — GDPR.
