# Montenegro — combined profile mapping

## Join keys & precedence

- **Primary join key: PIB** (Poreski identifikacioni broj, 8-digit) = company id =
  tax id, held by **CRPS**. CRPS registration number is the secondary key.
- **Precedence**: **CRPS** is authoritative for all identity, status, activity,
  address, ownership, and financials — but it was **unavailable** (portal 503;
  legacy domain parked). **data.gov.me Javna preduzeća** provides real
  public-enterprise names/status/type/founder/address/website (no PIB), joined to
  CRPS **by name** until CRPS is back.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.pib | crps_business_register | PIB | PIB | authoritative | portal down — null in example |
| registration.registration_number | crps_business_register | registarski_broj | PIB | authoritative | portal down |
| tax_identifiers.tax_id | crps_business_register | PIB | PIB | authoritative | = PIB |
| tax_identifiers.vat_id | crps_business_register | PDV_broj | PIB | authoritative | separate VAT |
| legal_identity.business_name | crps_business_register | naziv | PIB | authoritative | fallback: Javna preduzeća Naziv |
| legal_identity.legal_form | crps_business_register | oblik_organizovanja | PIB | authoritative | DOO/AD/OD/KD |
| status.status_text | crps_business_register | status | PIB | authoritative | Javna preduzeća Status as fallback |
| status.registration_date | crps_business_register | datum_registracije | PIB | authoritative |  |
| activity.activity_code | crps_business_register | sifra_djelatnosti | PIB | authoritative | KD ~NACE |
| registered_location.registered_address | crps_business_register | sjediste | PIB | authoritative | fallback: Javna preduzeća Adresa |
| public_enterprise.* | datagovme_javna_preduzeca | Tip/Osnivac/Website/PravniOsnov | Naziv | authoritative (public enterprises) | open dataset |
| owners[] | crps_business_register | osnivaci | PIB | authoritative | REDACT natural persons |
| financial_statements[] | crps_business_register | finansijski_izvjestaji | PIB | planning-only | filed at CRPS; NOT open; EUR |

## Freshness

- CRPS: live **when available** (was 503).
- data.gov.me Javna preduzeća: periodic (open).

## Missing-data notes

- **CRPS unavailable** at investigation time (portal 503; legacy domain parked) →
  no open bulk/API, no register values captured (`insufficient_transport_info`).
- **No open financials** — filed at CRPS, not published.
- **data.gov.me has no full register** — only public enterprises + statistics.
- **Owners** redacted as personal data (the state founder is a legal entity, kept).
