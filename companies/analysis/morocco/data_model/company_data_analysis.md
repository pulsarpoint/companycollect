# Company Data Analysis For Morocco

## Summary

Morocco's official register is **OMPIC** (Registre Central du Commerce), keyed on the
**ICE** (Identifiant Commun de l'Entreprise, 15-digit **unified** company id), with
the **RC number** (per court) and **IF** (tax id). OMPIC serves company data via
**directinfo.ma**, but the **free search is reCAPTCHA-gated** and **detailed data +
Bilans (financial statements) + the OMPIC API are paid/subscription** — there is **no
open bulk**.

The one genuinely **open** source is the **Casablanca Stock Exchange** — issuer
listing + financial publications — **verified live** (AFMA SA, Afric Industries SA,
Alliances Développement Immobilier SA, Atlanta Sanad). **data.gov.ma** is real CKAN
but has **no company register** (statistics only). So there is **no open bulk
corporate register and no open private financials** — ingestion is `blocked_payment`
(OMPIC) + open-for-listed (Bourse). Currency **MAD**; managers/shareholders are
personal data (Law 09-08). No OMPIC per-company values were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| casablanca_bourse | Casablanca Stock Exchange | ready | **open** | public disclosure | Listed identity + financials |
| ompic_directinfo | OMPIC — Registre Central du Commerce | blocked_payment | reCAPTCHA search; paid detail + API | restricted | Corporate identity + financials |

(data.gov.ma is recorded in discovery as a statistics-only secondary source.)

## What Each Source Contributes

- **casablanca_bourse** — open listed-company directory (name, ticker, sector) +
  issuer financial publications (MAD). Verified live (AFMA SA, etc.).
- **ompic_directinfo** — the canonical corporate record (ICE, RC, IF, raison sociale,
  forme juridique, statut, capital, activité, adresse, dirigeants, Bilans), via a
  reCAPTCHA-gated search + paid detail/API. Field model from public knowledge.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **ICE** with sections: `registration`
(ice/rc_number), `tax_identifiers` (if_tax_id; TVA tied to it), `legal_identity`,
`status`, `activity` (OMPIC object / Bourse sector), `registered_location`, `capital`
(MAD, paid), `officers` (redacted, paid), `listing` (Bourse, open),
`financial_statements[]` (Bourse listed / OMPIC Bilans paid), and
`source_provenance[]`. The example uses the Bourse-verified **AFMA SA** with OMPIC
identifiers null.

## Join And Precedence Rules

- **ICE** is the unified corporate key; **RC** is the registration; **IF** links tax;
  **Casablanca ticker** keys the listed entity.
- **OMPIC** authoritative for corporate identity + financials (gated/paid);
  **Casablanca Bourse** for listed (open).

## Missing Or Restricted Data

- **No open bulk corporate register; no open private financials** — OMPIC
  reCAPTCHA-gated + paid; only the Bourse (listed) is open.
- **No company dataset on data.gov.ma** (statistics only).
- **No separate VAT number** (TVA tied to the IF).
- **Incorporation/dissolution dates** only in gated OMPIC detail.
- **Dirigeants/associés** redacted as personal data (Law 09-08).

## Common Mapper Notes

`company_id == ICE`; `registration_number == Numéro RC`; `tax_id == IF`; no separate
`vat_id`. The blocker is **OMPIC reCAPTCHA + paid**; the open path is the **Casablanca
Stock Exchange** (listed). Currency **MAD**. See `common_field_mapping_suggestions.md`.
