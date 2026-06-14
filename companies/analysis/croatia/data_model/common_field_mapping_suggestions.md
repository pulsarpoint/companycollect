# Croatia — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Croatia's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Croatia source path | Notes |
|---|---|---|
| company_id | registration.oib | 11-digit tax id; = VAT root. |
| registration_number | registration.mbs (court register) | OIB also serves as a key. |
| tax_id | registration.oib | No separate tax id; OIB is the fiscal id. |
| vat_id | registration.vat_id ("HR" + oib) | Derived. |
| legal_name | legal_identity.name | tvrtka/naziv. |
| status | status.derived | aktivan/u likvidaciji/brisan. |
| legal_form | legal_identity.legal_form | d.o.o./j.d.o.o./d.d./obrt. |
| incorporation_date | (sudski_registar datum_osnivanja) | open. |
| dissolution_date | (brisanje) | open. |
| registered_address | registered_location.* | open (register). |
| activity_code | activity.nkd_codes | NKD (Croatian NACE) where coded; partly free text. |
| financials | financial_statements[] | **OPEN structured CSV** (FINA RGFI) — free (login); micro/small abbreviated. |
| officers | officers_and_owners[] (type=officer) | management board/directors — OPEN (PII). |
| owners | officers_and_owners[] (type=owner) + beneficial_owners[] (restricted) | Members/owners OPEN; beneficial ownership restricted. |
| source_provenance | source_provenance[] | per-source + access flag. |

## Cross-country notes for a future mapper

- **Croatia is an open, clean-key case** (Belgium/Poland tier, free-but-registered): a cross-country mapper
  gets identity + **structured financials** + activity + capital + **officers AND owners** for free, joined on
  one **OIB** (= VAT root). Notably the register exposes **both members/owners and management openly** (osobe).
- **No separate tax id** — OIB is the company id, fiscal id, and VAT root; MBS is the court-register number.
- **Activity code (NKD)** is available where coded (partly free text) — mostly present.
- **Financials**: open structured CSV (FINA RGFI); a cross-country `financials` mapper must tolerate
  abbreviated forms for micro/small (revenue null) and the **EUR/HRK currency boundary at 2023**; large-company
  full data may need the paid FINA product.
- **Beneficial ownership (RSV)** restricted; open ownership = the register's members.
- **Access caveat**: both core sources need a **free registration/account** (sudreg key; FINA login).
