# France Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **France's defining trait: a
single clean join key (`siren`)** across every source — no fuzzy matching (contrast Germany = no key,
Spain = sparse CIF). Financials are **open** but coverage is **partial** (confidentiality option).

## Identity / legal / activity / location

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.siren | insee_sirene | siren | **PK (all sources)** | monthly+daily | ODbL / public | Universal key |
| registration.siege_siret | insee_sirene / recherche_entreprises | siret (siège) | — | daily | ODbL/open | HQ establishment |
| registration.vat_id | derived | FR+key+siren | — | — | — | Computed |
| legal_identity.nom_complet | recherche_entreprises / insee_sirene | nom_complet / denomination | — | daily | open/ODbL | Prefer Sirene for authoritative name |
| legal_identity.nature_juridique | recherche_entreprises / insee_sirene | nature_juridique | — | daily | open/ODbL | INSEE cat. juridique code |
| legal_identity.objet_social | inpi_rne | objet | siren | daily | INPI open | free text |
| status.etat_administratif | insee_sirene / recherche_entreprises | etat_administratif | — | daily | ODbL/open | A/C |
| status.derived | bodacc | jugement (procédure collective) | registre→siren | daily | Licence Ouverte 2.0 | insolvency signal |
| activity.naf_rev2 / naf2025 | recherche_entreprises / insee_sirene | activite_principale(_naf25) | — | daily | open/ODbL | **clean codes (France has these)** |
| registered_location.* | recherche_entreprises / insee_sirene | siege.* | — | daily | open/ODbL | address + geo |
| capital.* | inpi_rne | montantCapital | siren | daily | INPI open | register capital, not accounts |
| size.categorie_entreprise / tranche_effectif | recherche_entreprises / insee_sirene | categorie_entreprise / tranche_effectif | — | daily | open/ODbL | band codes |
| officers[] | recherche_entreprises / inpi_rne | dirigeants / représentants | siren | daily | open/INPI · **PII** | INPI richer (roles) |
| beneficial_owners[] | inpi_rne | RBE | siren | — | **restricted** | **planning-only** |
| establishments[] | insee_sirene | StockEtablissement | siren | monthly | ODbL | SIRET sites |
| events[] | bodacc | annonces | registre→siren | daily | Licence Ouverte 2.0 | lifecycle |

## Financial statements (open, multi-source)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] (headline) | recherche_entreprises | finances{year:{ca,resultat_net}} | siren | daily | **open, no auth** | **Default at scale** — revenue + net income, free |
| financial_statements[] (full) | inpi_comptes_annuels | bilan + compte de résultat | siren | daily | open, **free INPI account** | Use for full balance sheet + income statement |

### Financial source precedence
1. **recherche_entreprises `finances`** — zero-friction headline **revenue + net income** for the whole
   spine, no auth. Default for breadth.
2. **inpi_comptes_annuels** — when you need the **full statement** (total assets, equity, liabilities,
   operating result, depreciation). Free INPI account; non-confidential only.
3. (Restricted, planning-only) **API Entreprise** brokers DGFIP chiffres d'affaires + Banque de France
   bilans (3 yrs) — richer/authoritative but **habilitation only**; not for open ingestion.

Dedupe financial records on `siren + fiscal_year + accounts_type`; prefer the INPI full statement over
the headline pair for the same year (keep both `source`s). `revenue`/`net_income`/etc. are **nullable**
(confidentiality option; `simplifié` discloses fewer lines). Currency usually EUR — store it.

## Join & precedence summary

- **Single join key `siren`** everywhere (SIRET for establishments; BODACC needs SIREN parsed from
  `registre`). No fuzzy matching required — France's big advantage over DE/ES.
- **Authority**: Sirene (INSEE) authoritative for identity/activity/status; INPI RNE authoritative for
  capital/dirigeants/accounts; Recherche API is a convenient daily-rebuilt merge of both.
- **Freshness**: Sirene monthly stock + daily API; INPI + Recherche + BODACC daily.

## Missing / restricted data

- **Beneficial ownership (RBE)**: restricted since 2022 CJEU ruling → `beneficial_owners` is
  **planning-only**, not from open sources.
- **Financial coverage partial**: confidentiality option removes many small firms; micro-entrepreneurs
  lack DGFIP revenue. Do not treat missing financials as zero.
- **Authoritative/richer financials** (DGFIP CA, Banque de France bilans) only via restricted API Entreprise.
- **PII**: dirigeants and (restricted) beneficial owners — GDPR; honor `statut_diffusion=P`.
