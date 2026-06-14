# Romania Company Profile — Mapping Report

Romania is **best-in-class fully-open**: a complete identified register (ONRC
OD_FIRME + companion CSVs on data.gov.ro) **and** free structured financials
(ANAF `/bilant`). Two identifiers carry the joins: **CUI** (fiscal code → ANAF /
VAT) and **COD_INMATRICULARE** (register number → ONRC companion CSVs). OD_FIRME
holds both and is the bridge. Only **shareholders/beneficial owners** are not
open.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.cui | onrc_od_firme | CUI | CUI | register | 0/null for some PF |
| registration.cod_inmatriculare | onrc_od_firme | COD_INMATRICULARE | COD_INMATRICULARE | register | primary join key |
| registration.euid | onrc_od_firme | EUID | — | register | BRIS |
| tax_identifiers.vat_id | anaf_ws_tva | scpTVA → RO+CUI | CUI | ANAF | RO+CUI when scpTVA |
| tax_identifiers.vat_registered | anaf_ws_tva | found[].inregistrare_scop_Tva.scpTVA | CUI | ANAF | endpoint unconfirmed this run |
| tax_identifiers.fiscally_inactive | anaf_ws_tva | found[].stare_inactiv.statusInactivi | CUI | ANAF | |
| legal_identity.legal_name | onrc_od_firme | DENUMIRE | — | register > ANAF deni | |
| legal_identity.legal_form | onrc_od_firme | FORMA_JURIDICA | — | register | |
| status.status_code | onrc_od_stare_firma | COD | COD_INMATRICULARE | register | 1048/1084/2069 |
| activity.caen_main | anaf_bilant | caen | CUI | ANAF (filing-time) | |
| activity.caen_authorized[] | onrc_od_caen_autorizat | COD_CAEN_AUTORIZAT | COD_INMATRICULARE | register | many per company |
| incorporation.registration_date | onrc_od_firme | DATA_INMATRICULARE | — | register | DD/MM/YYYY |
| registered_location.* | onrc_od_firme | ADR_* | — | register | reassemble |
| officers[] | onrc_od_reprezentanti_legali | PERSOANA_IMPUTERNICITA/CALITATE | COD_INMATRICULARE | register | OPEN but PII — redact |
| foreign_branches[] | onrc_od_sucursale_alte_state_membre | DENUMIRE_SUCURSALA/TARA | COD_INMATRICULARE | register | |
| financial_statements[] | anaf_bilant | i[].indicator/val_indicator | CUI | ANAF | RON; 2014-2024 |
| ownership.shareholders[] | onrc_portal_recom | asociati/actionari | CUI | PLANNING-ONLY | paid; not open |
| ownership.beneficial_owners[] | onrc_rbr | beneficiari[] | CUI | PLANNING-ONLY | restricted; PII |

## Source Precedence

1. **ONRC OD_FIRME + companions** (data.gov.ro) — authoritative for identity,
   form, status, activities, address, officers, branches. Open.
2. **ANAF /bilant** — authoritative for **financials** (and a filing-time main
   CAEN). Open, free, 2014–2024.
3. **ANAF ws/tva** — current VAT/fiscal status enrichment (endpoint version to be
   reconfirmed). Open.
4. **ONRC portal / RBR** — **shareholders / beneficial owners** only; paid /
   restricted → **planning-only**.

When names differ, prefer the **ONRC** register name (DENUMIRE) over ANAF `deni`.

## Join Keys

- **CUI** ↔ ANAF financials/VAT. **COD_INMATRICULARE** ↔ all ONRC companion CSVs.
- OD_FIRME carries both, so: load OD_FIRME → join status/CAEN/officers/branches
  on COD_INMATRICULARE → enrich financials/VAT on CUI.

## Missing / Restricted

- **Shareholders & beneficial owners**: not open (paid ONRC portal / restricted
  RBR).
- **Detailed share capital**: paid portal; free proxy = ANAF `I11` (paid-up
  capital).
- **Dissolution date**: not a dedicated field — only implied by status code.
- **Status nomenclator**: full code→label table to be obtained from ONRC.
- **Officers PII**: open but GDPR — redact names/birth fields.
