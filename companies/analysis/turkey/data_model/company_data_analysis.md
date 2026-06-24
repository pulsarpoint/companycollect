# Company Data Analysis For Turkey

## Summary

Turkey offers a **free per-company registry lookup** (MERSIS) with **no open bulk**,
company **events** via the Trade Registry Gazette, and **public financials only for
listed companies** (KAP). Keyed on the **MERSIS number** (16-digit company id) and
**VKN** (10-digit tax id), a per-company profile can be built: identity (title,
type, NACE, address, status) + listed financials (TRY). The example uses real KAP
data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| mersis_registry | MERSIS (central registry) | insufficient_transport_info | free per-company; no open bulk | free/no-bulk | Authoritative identity |
| kap_disclosure | KAP (Public Disclosure Platform) | ready | public (listed) | public disclosure | Listed-company financials |
| ticaret_sicil_gazetesi | Trade Registry Gazette | insufficient_transport_info | public search | public gazette | Company events / officers |
| gib_vkn | GİB (VKN lookup) | insufficient_transport_info | per-company | free | VKN / KDV verification |

## What Each Source Contributes

- **mersis_registry** — authoritative identity (16-digit MERSIS no, title, VKN,
  trade-registry no, NACE, address, type, status); free per-company query, no open
  bulk.
- **kap_disclosure** — listed-company financial statements (Bilanço/Gelir Tablosu,
  Turkish IFRS, TRY) + disclosures. Verified live (808 listed companies extracted).
- **ticaret_sicil_gazetesi** — company events (incorporation/amendment/dissolution)
  and directors/shareholders (personal data, KVKK).
- **gib_vkn** — VKN / KDV (VAT) taxpayer verification.

## Proposed Country Company Profile

A single object keyed on `registration.mersis_no` (+ VKN, KAP id):

- `registration` — MERSIS no, trade-registry no, KAP id.
- `tax_identifiers` — tax_id = VKN; vat_id null (no separate VAT number).
- `legal_identity` — name, company type.
- `status`, `activity` (NACE), `registered_location`.
- `documents[]` — gazette announcements.
- `officers[]` — directors/shareholders (gazette; personal data).
- `financial_statements[]` — KAP (listed only; TRY).
- `source_provenance[]`.

## Join And Precedence Rules

- **Keys**: MERSIS no (16-digit) + VKN (10-digit); KAP id/name for listed.
- **Precedence**: MERSIS (identity) > KAP (listed financials) > gazette (events/
  officers) > GİB (tax verification).
- **No VAT number** — the VKN is the tax id.

## Missing Or Restricted Data

- **Open bulk register** — none (per-company query only).
- **Private-company financials** — listed-only (KAP).
- **Directors/shareholders** — gazette; personal data (KVKK).
- **No separate VAT number** — the VKN is used.

## Common Mapper Notes

- Map `company_id` -> MERSIS no; `tax_id` -> VKN; `vat_id` not available.
- Map `financials` from KAP (listed; TRY).
- Treat MERSIS as per-company (no enumeration); redact gazette officer data (KVKK).
