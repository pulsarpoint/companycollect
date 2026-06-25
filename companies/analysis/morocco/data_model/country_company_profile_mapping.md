# Morocco — combined profile mapping

## Join keys & precedence

- **Primary join key: ICE** (Identifiant Commun de l'Entreprise, 15-digit unified id).
  The **RC number** (per court) and **IF** (tax id) are secondary keys; for listed
  companies the **Casablanca ticker / ISIN** is an additional key (join to OMPIC by
  company name / ICE).
- **Precedence**: **OMPIC** (directinfo) is authoritative for corporate identity,
  status, capital, managers, and Bilans — but **reCAPTCHA-gated** (search) and
  **paid** (detail). **Casablanca Bourse** is authoritative for the **listed** subset
  (open).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.ice | ompic_directinfo | ice | ICE | authoritative | gated/paid |
| registration.rc_number | ompic_directinfo | rc_number | ICE | authoritative | per court |
| tax_identifiers.if_tax_id | ompic_directinfo | if_tax_id | ICE | authoritative | tax id |
| tax_identifiers.vat_id | n/a | — | — | n/a | TVA tied to the IF |
| legal_identity.legal_name | ompic_directinfo | raison_sociale | ICE | authoritative | Bourse name as alt (listed) |
| legal_identity.legal_form | ompic_directinfo | forme_juridique | ICE | authoritative | SA/SARL/... |
| status.status_text | ompic_directinfo | statut | ICE | authoritative | en activité/liquidation/radiée |
| activity.activity_object | ompic_directinfo | activite | ICE | authoritative | NMA |
| activity.bourse_sector | casablanca_bourse | sector | ticker | authoritative (listed) | open |
| registered_location.registered_address | ompic_directinfo | adresse | ICE | authoritative | gated/paid |
| capital.capital_social | ompic_directinfo | capital_social | ICE | authoritative (paid) | MAD |
| officers[] | ompic_directinfo | dirigeants | ICE | authoritative (paid) | REDACT (Law 09-08) |
| listing.* | casablanca_bourse | ticker/isin/sector | ticker | authoritative (listed) | OPEN |
| financial_statements[] | casablanca_bourse / ompic_directinfo | publications / Bilans | ticker/ICE | authoritative | Bourse open (listed); OMPIC Bilans paid |

## Freshness

- OMPIC: **live** (reCAPTCHA/paid). Casablanca Bourse: **event-driven/quarterly** (open).

## Missing-data notes

- **No open bulk corporate register; no open private financials** — OMPIC
  reCAPTCHA-gated + paid; only the Bourse (listed) is open.
- **data.gov.ma has no company register** (statistics only).
- **No separate VAT number** (TVA tied to the IF).
- **Dirigeants/associés** redacted as personal data (Law 09-08).
- **No OMPIC per-company values captured** (reCAPTCHA/paywall not bypassed); listed
  identity from the Bourse.
