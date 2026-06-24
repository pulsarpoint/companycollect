# Turkey Company Profile — Source Mapping

> Keyed on the **MERSIS number** (16-digit company id) + **VKN** (10-digit tax id).
> Turkey has VAT (KDV) but **no separate VAT number** — the VKN is the tax id.
> Identity is via **MERSIS** (free per-company query, no open bulk) + the **Trade
> Registry Gazette** (events). Financials are public only for **listed** companies
> via **KAP** (TRY). Directors/shareholders are personal data (KVKK).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.mersis_no | mersis_registry | mersis.mersis_no | mersis_no | live | free/no-bulk | Company id. |
| registration.trade_registry_no | mersis_registry | mersis.ticaret_sicil_no | — | live | free/no-bulk | Per office. |
| registration.kap_id | kap_disclosure | company.kap_id | kap_id | live | public | Listed only. |
| tax_identifiers.tax_id | mersis_registry | mersis.vkn | vkn | live | free/no-bulk | VKN. |
| tax_identifiers.vat_id | — | — | — | — | not available | VKN serves as VAT id. |
| legal_identity.legal_name | mersis_registry | mersis.unvan | — | live | free/no-bulk | (KAP name as fallback). |
| legal_identity.company_type | mersis_registry | mersis.sirket_turu | — | live | free/no-bulk | A.Ş./Ltd. Şti. |
| status.status | mersis_registry | mersis.durum | — | live | free/no-bulk | active/dissolved. |
| activity.nace_code | mersis_registry | mersis.nace | — | live | free/no-bulk | NACE. |
| registered_location.registered_address | mersis_registry | mersis.adres | — | live | free/no-bulk | |
| documents[] | ticaret_sicil_gazetesi | gazette announcements | mersis_no/sicil | continuous | public gazette | Events. |
| officers[] | ticaret_sicil_gazetesi | ilan_metni | sicil/name | continuous | public gazette | PLANNING-ONLY; personal data (KVKK) — redact. |
| financial_statements[] | kap_disclosure | financials.balance_income | kap_id/vkn/name | quarterly | public | LISTED only; TRY. |

## Source precedence

1. **mersis_registry** — authoritative identity (free per-company; no open bulk).
2. **kap_disclosure** — listed-company financials (open; verified 808 companies).
3. **ticaret_sicil_gazetesi** — company events + officers (gazette; personal data).
4. **gib_vkn** — VKN / KDV taxpayer verification.

Conflict rules:
- **Identity**: MERSIS is authoritative; KAP name is a cross-check for listed.
- **Financials**: KAP (listed only); private companies have none.

## Join keys

- **MERSIS no** (16-digit) + **VKN** (10-digit) across registry/gazette/tax; **KAP
  id**/name for listed financials. The VKN is the tax id; no separate VAT number.

## Missing / restricted data

- **Open bulk register** — none (MERSIS per-company query only).
- **Private-company financials** — only listed (KAP).
- **Directors/shareholders** — gazette/trade-registry; personal data (KVKK).
- **No separate VAT number** — the VKN is used.
