# Company Data Analysis For Iceland

## Summary

Iceland offers a **free per-company company-register lookup** but **no open bulk**,
and **paid financials**. The **Fyrirtækjaskrá** (Skatturinn) free overview, keyed on
the **kennitala** (10-digit; company id = tax id), exposes name, registered
address, municipality, legal form (rekstrarform), ÍSAT activity, VAT/VSK status,
and the board chair. Bulk extracts and certified certificates are **paid**; there
is no open dataset/API to enumerate. Financial statements (**Ársreikningaskrá**)
are filed for public disclosure but **paid** per-document. The example uses real
register data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| skatturinn_fyrirtaekjaskra | Fyrirtækjaskrá (company register) | recommended | free per-company; paid bulk | free overview / paid bulk | Authoritative open identity (per-company) |
| arsreikningaskra | Ársreikningaskrá (Annual Accounts) | blocked_payment | paid per-document | restricted | Financials (paid) |

## What Each Source Contributes

- **skatturinn_fyrirtaekjaskra** — the authoritative register: kennitala, legal
  name, registered address (lögheimili), municipality (sveitarfélag), legal form
  (rekstrarform), ÍSAT activity, VAT/VSK status, and the board chair (forráðamaður,
  personal data). Verified live (JBT Marel ehf., Icelandair ehf., a húsfélag). Free
  per-company; **no open bulk** (paid extracts/certificates).
- **arsreikningaskra** — filed annual accounts (revenue, profit, assets, equity),
  keyed on kennitala, per fiscal year, ISK. Public disclosure but **paid**
  retrieval; planning-only.

## Proposed Country Company Profile

A single object keyed on `registration.kennitala`:

- `registration` — kennitala.
- `tax_identifiers` — tax_id = kennitala; vat_id = VSK (separate; status only).
- `legal_identity` — name, legal form.
- `status` — registered / afskráð.
- `activity` — ÍSAT code.
- `registered_location` — address, municipality.
- `officers[]` — chair (GDPR; full board paid).
- `financial_statements[]` — paid (Ársreikningaskrá), ISK; planning-only.
- `source_provenance[]`.

## Join And Precedence Rules

- **Join key**: the kennitala across both Skatturinn registers (and = the tax id).
- **Precedence**: Fyrirtækjaskrá (open identity) > Ársreikningaskrá (paid
  financials). No conflicting sources.
- **VAT (VSK)** is a separate registration; only the status is open.

## Missing Or Restricted Data

- **Open bulk / enumeration** — none (per-company lookup; bulk paid).
- **Financial statements** — paid (Ársreikningaskrá).
- **Incorporation date, full board, certified certificates** — paid certificate.
- **Beneficial owners** — register exists but not openly published.
- **Personal data** — board chair (forráðamaður), GDPR; redact.

## Common Mapper Notes

- Map `company_id`/`registration_number`/`tax_id` all to the kennitala; `vat_id` is
  a separate VSK registration (status only).
- Map `financials` from the paid Ársreikningaskrá (ISK).
- There is no open dataset to iterate — lookups need seed kennitalas; redact the
  chair (GDPR).
