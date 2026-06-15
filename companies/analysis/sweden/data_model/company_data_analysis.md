# Company Data Analysis For Sweden

## Summary

Sweden moved from a **paid** company-data regime to a **free** one on **26 June 2025**, when
Bolagsverket and SCB launched the *Värdefulla datamängder* ("valuable datasets") programme under the
EU Open Data Directive (2019/1024) high-value-datasets rule. As a result you can build a **rich,
mostly-complete company profile** for Sweden from two official, free sources, joined on
**organisationsnummer**:

- **Identity, legal form, status, registered address, dates, and financial statements** from
  **Bolagsverket Värdefulla datamängder** (base data via `/organisationer`; iXBRL annual reports via
  `/dokumentlista` + `/dokument/{id}`).
- **Full company + workplace universe, CFAR workplace ids, municipality/county geography, SNI,
  employee size-class, and VAT/employer/F-tax register flags** from **SCB Företagsregistret / FDB**
  (CC0).

The profile's notable strengths are **machine-readable financials** (iXBRL tagged to the Swedish
K2/K3 taxonomies) and **workplace-level (arbetsställe) granularity**. The notable gaps are
**officers and beneficial ownership** (not in the free open set) and **exact employee counts** (only
SCB size-class bands).

**Important caveat for implementation:** both official APIs are auth-gated and **no authenticated
records were pulled** during discovery (Bolagsverket returns HTTP 401 *Missing Credentials*; SCB
needs a client certificate). Every field in these catalogs is **documented-but-unverified** and must
be confirmed against a real credentialed response before the parser hardcodes keys.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| `bolagsverket_vdm` | Bolagsverket VDM — /organisationer base data | recommended | OAuth2 (free, gated) | Free/no-contract (EU HVD) | **Primary** — identity, legal form, status, address, dates |
| `bolagsverket_annual_reports` | Bolagsverket VDM — iXBRL annual reports | recommended | OAuth2 (free, gated) | Free/no-contract; verify doc files | **Primary financials** |
| `scb_fdb` | SCB Företagsregistret / FDB | recommended | cert → API key 2026-09 | CC0 1.0 | **Secondary/seed** — universe, workplaces, SNI, size-class, flags, geography |
| `dataportal_se` | dataportal.se DCAT catalog | useful_secondary | public | metadata | Provenance/license discovery only (no company data) |
| Bolagsverket paid XML packet | legacy paid bulk/API | blocked_by_payment | paid | commercial | Superseded by free VDM — not cataloged |
| Verklig huvudman (UBO) | beneficial ownership register | blocked_by_license_uncertainty | restricted | restricted | Out of scope — see Missing/Restricted |
| Commercial aggregators | allabolag, bolagsapi.se, apiverket.se, … | useful_secondary | paid | vendor terms | Repackage official data — fallback only |

## What Each Source Contributes

- **bolagsverket_vdm** — the legal register. Authoritative legal name, legal form
  (AB/HB/KB/Enskild firma/Ekonomisk förening), status (registrerad/avregistrerad/konkurs/likvidation),
  registration/deregistration dates, and registered postal address.
- **bolagsverket_annual_reports** — the only financial source. Annual reports as **iXBRL** tagged to
  the K2/K3 taxonomies, giving income statement (Nettoomsättning, Rörelseresultat, Årets resultat …)
  and balance sheet (Summa tillgångar/eget kapital/skulder). Two-step retrieval; download is a ZIP of
  an iXBRL instance. Free coverage = digitally filed reports.
- **scb_fdb** — the statistical register and best **seed** for the full orgnr universe. Adds what
  Bolagsverket does not break out: **CFAR workplaces**, **kommun/län geography**, **fuller SNI**
  (company + workplace), **employee size-class**, and **VAT/employer/F-tax** register flags.
- **dataportal_se** — national DCAT catalog; confirms publishers and carries the formal license
  string. No company records.

## Proposed Country Company Profile

`country_company_profile.schema.json` models a Sweden-specific object joined on
**organisationsnummer**, with these sections:

- `registration` — orgnr (+ display form), VAT, and the SCB register flags.
- `legal_identity` — legal name + legal form (Bolagsverket authoritative; SCB cross-check).
- `status` — raw status, derived active flag, incorporation/dissolution dates.
- `activity` — SNI codes (array) + employee size-class.
- `registered_location` — address (Bolagsverket) enriched with kommun/län (SCB).
- `local_units[]` — CFAR workplaces (SCB-only), each with address, SNI, size-class, main/subsidiary.
- `financials[]` — per fiscal year from iXBRL, with currency, K2/K3 framework, and a `raw_xbrl_concepts`
  map for re-normalization.
- `documents[]` — filed annual-report document references.
- `source_provenance[]` — one entry per contributing source.

`country_company_profile.example.json` is a shape-only illustrative record (clearly marked; no
fetched values).

## Join And Precedence Rules

- **Join key:** organisationsnummer everywhere; CFAR is the workplace sub-key (SCB).
- **Legal facts** (name, form, status, dates) → **Bolagsverket** authoritative.
- **Geography, workplaces, SNI breadth, employee size-class, register flags** → **SCB** authoritative.
- **Financials** → **Bolagsverket iXBRL** only.
- **VAT** → prefer sourced; else derive `SE`+orgnr+`01` and mark derived; active only if SCB VAT flag set.
- **Freshness:** Bolagsverket real-time; SCB nightly/weekly. On conflict prefer the real-time legal
  register for legal facts, SCB for statistical/geographic facts.

## Missing Or Restricted Data

Unavailable from open/public sources:

- **Officers / board members** — not in the free VDM/SCB datasets.
- **Beneficial ownership (verklig huvudman)** — register exists at Bolagsverket but is **restricted**
  and not part of the free open-API set. *Planning-only; out of scope for open ingestion.* No field
  catalog was fabricated for it (no documented fields available).
- **Exact employee headcount** — only SCB size-class bands.
- **Pre-computed financial ratios / long historical financial series** — derive ratios from iXBRL;
  history limited to digitally filed reports.

Available only from paid/restricted sources:

- **Bolagsverket XML bulk packet / legacy paid API** (~SEK 6,250 onboarding + usage) — provides a
  one-shot full register, but is **superseded** by the free VDM API for open use. Planning-only.
- **Commercial aggregators** (allabolag, bolagsapi.se, apiverket.se, foretagsapi.se, OpenCorporates,
  Apify) — repackage the same official data under vendor terms. Fallback/comparison only.

## Common Mapper Notes

A future cross-country mapper can map company_id, registration_number, tax_id, vat_id, legal_name,
status, legal_form, incorporation/dissolution dates, registered_address, activity_code (SNI), and
financials. It should set `officers` and `owners` to `not_available_in_open_sources` for Sweden.
Sweden-specific richness a thin common layer would drop — **CFAR workplaces**, **F-skatt/employer/VAT
flags**, **K2/K3 framework**, **kommun/län** — should stay in the country-specific model. See
`common_field_mapping_suggestions.md`.
