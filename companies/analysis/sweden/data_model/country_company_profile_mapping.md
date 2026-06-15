# Sweden Company Profile — Source Mapping & Precedence

Join key across every source: **organisationsnummer** (digits-only canonical form).
For workplaces, the secondary key is **CFAR-nummer** (SCB only).

## Source legend

| Slug | Source | Access | License | Freshness |
|---|---|---|---|---|
| `bolagsverket_vdm` | Bolagsverket VDM — /organisationer base data | OAuth2 (free, gated) | Free/no-contract (EU HVD); confirm | real-time |
| `bolagsverket_annual_reports` | Bolagsverket VDM — iXBRL annual reports | OAuth2 (free, gated) | Free/no-contract; verify doc files | per filing |
| `scb_fdb` | SCB Företagsregistret / FDB | client certificate → API key 2026-09 | CC0 1.0 | nightly/weekly |
| `dataportal_se` | dataportal.se DCAT catalog | public | metadata | n/a |

## Field mapping

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.organisationsnummer | scb_fdb / bolagsverket_vdm | organisationsnummer | yes | Either (identical) | Canonical digits-only |
| registration.organisationsnummer_display | derived | — | — | — | NNNNNN-NNNN |
| registration.vat_id | derived / scb_fdb(flag) / bolagsverket_vdm | momsregistreringsnummer | orgnr | Sourced > derived | Derive SE+orgnr+01 only if no sourced value; mark derived |
| registration.vat_registered | scb_fdb | foretag.moms_flagga | orgnr | SCB | Confirms VAT is active |
| registration.f_tax_registered | scb_fdb | foretag.fskatt_flagga | orgnr | SCB | Active-business signal |
| registration.employer_registered | scb_fdb | foretag.arbetsgivare_flagga | orgnr | SCB | — |
| legal_identity.legal_name | bolagsverket_vdm | organisationsnamn | orgnr | **Bolagsverket > SCB** | Bolagsverket is legal-name authority |
| legal_identity.name_scb | scb_fdb | foretag.foretagsnamn | orgnr | fallback | Cross-check only |
| legal_identity.legal_form | bolagsverket_vdm | juridisk_form | orgnr | **Bolagsverket > SCB** | Keep SCB cross-walk |
| status.status_raw | bolagsverket_vdm | status | orgnr | Bolagsverket | konkurs/likvidation preserved |
| status.incorporation_date | bolagsverket_vdm | registreringsdatum | orgnr | Bolagsverket | — |
| status.dissolution_date | bolagsverket_vdm | avregistreringsdatum | orgnr | Bolagsverket | — |
| activity.sni_codes | scb_fdb / bolagsverket_vdm | foretag.sni_kod / naringsgrenskod | orgnr | **SCB > Bolagsverket** | SCB fuller; union both, dedupe |
| activity.employees_size_class | scb_fdb | foretag.storleksklass_anstallda | orgnr | SCB only | Band, never exact |
| registered_location.street_address | bolagsverket_vdm | postadress_organisation.gatuadress | orgnr | Bolagsverket | — |
| registered_location.postal_code/town | bolagsverket_vdm / scb_fdb | postnummer / postort | orgnr | Bolagsverket, SCB fallback | — |
| registered_location.municipality (kommun) | scb_fdb | foretag.kommun | orgnr | **SCB only** | Authoritative geography |
| registered_location.county (län) | scb_fdb | foretag.lan | orgnr | **SCB only** | Authoritative geography |
| local_units[] | scb_fdb | arbetsstalle.* | orgnr → cfar | **SCB only** | CFAR workplaces; not in Bolagsverket |
| financials[] | bolagsverket_annual_reports | iXBRL K2/K3 concepts | orgnr → fiscal_year | **Bolagsverket only** | Parse iXBRL; digital filings only |
| documents[] | bolagsverket_annual_reports | dokumentlista[] | orgnr | Bolagsverket only | — |
| source_provenance[] | all | — | — | — | One entry per contributing source |

## Precedence rules (summary)

1. **Identity / legal name / legal form / status / dates** → **Bolagsverket VDM** is authoritative
   (the legal register). SCB values are cross-checks / fallbacks.
2. **Geography (kommun/län), workplaces (CFAR), employee size-class, register flags (VAT/employer/
   F-tax), full SNI** → **SCB FDB** is authoritative (the statistical register adds what Bolagsverket
   does not break out).
3. **Financials** → **Bolagsverket annual reports (iXBRL)** is the only source. No financials exist in
   SCB.
4. **VAT number** → prefer a sourced value; otherwise derive `SE`+orgnr+`01` and mark it derived;
   only treat as active when the SCB VAT flag is set.
5. **License provenance** → record the exact Bolagsverket VDM reuse string from `dataportal_se`.

## Missing-data notes

- All values currently **uncaptured**: both official sources are auth-gated and no records were
  pulled. Field keys/casing/nesting are documented-but-unverified until a credentialed pull.
- **No beneficial ownership** (verklig huvudman) in open sources — restricted (see analysis).
- **Financial history** limited to digitally filed annual reports; older paper filings absent.
- **Exact employee headcount** never available — only SCB size-class bands.
- **No pre-computed ratios** — derive (e.g. soliditet = equity/assets) downstream.
