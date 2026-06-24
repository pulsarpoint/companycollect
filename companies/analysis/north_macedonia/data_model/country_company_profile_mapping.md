# North Macedonia — combined profile mapping

## Join keys & precedence

- **Primary join key: ЕМБС** (7-digit unique entity registration number) = company
  id, held by the Central Registry. **ЕДБ** (13-digit) is the tax number; **ДДВ**
  is the VAT registration (UJP).
- **Precedence**: the **Central Registry (CRM)** is authoritative for everything —
  identity, status, activity, address, capital, ownership, and **annual financial
  statements**. Only a **free basic search** is open; detailed/bulk data and
  financials are **paid**.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.embs | crm_trade_registry | EMBS | ЕМБС | authoritative | free basic / paid detail |
| tax_identifiers.edb_tax_id | crm_trade_registry | EDB | ЕМБС | authoritative | 13-digit tax id |
| tax_identifiers.vat_id | ujp_tax | ДДВ број | ЕДБ | UJP | VAT registration |
| legal_identity.business_name | crm_trade_registry | naziv | ЕМБС | authoritative | Cyrillic/Latin/Albanian |
| legal_identity.legal_form | crm_trade_registry | pravna_forma | ЕМБС | authoritative | ДОО/ДООЕЛ/АД |
| status.status_text | crm_trade_registry | status | ЕМБС | authoritative | активен/ликвидација/стечај/избришан |
| activity.activity_code | crm_trade_registry | dejnost_NKD | ЕМБС | authoritative | НКД ~NACE |
| registered_location.registered_address | crm_trade_registry | sediste_adresa | ЕМБС | authoritative |  |
| capital.registered_capital | crm_trade_registry | osnovna_glavnina | ЕМБС | authoritative (paid) | MKD |
| owners[] | crm_trade_registry | upraviteli_osnovaci | ЕМБС | authoritative (paid) | REDACT natural persons |
| financial_statements[] | crm_annual_accounts | баланс/успех | ЕМБС | authoritative (paid) | MKD; planning-only |

## Freshness

- CRM Trade Registry: **live**. CRM Annual Accounts: **annual**. Both **paid** for
  detail/bulk.

## Missing-data notes

- **No open bulk register; no open financials** — CRM commercially distributes the
  data (`blocked_payment`); only a free basic search is open.
- **Environment block**: crm.com.mk firewalled (DNS resolves; TCP/HTTP timeout) →
  no values captured; model from public docs.
- **Managers/founders** redacted as personal data (and behind paid access).
