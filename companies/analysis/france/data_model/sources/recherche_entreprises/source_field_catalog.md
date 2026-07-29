# API Recherche d'Entreprises — Field Catalog

## Source Summary

- Country: France
- Source type: official_aggregator_api (DINUM / Annuaire des Entreprises)
- Organization: DINUM
- URL: `https://recherche-entreprises.api.gouv.fr/search?q=...` (no auth)
- License: open public service; underlying Sirene data is Open Licence 2.0 plus INPI terms
- Access: public, **no key**; ~7 req/s/IP
- Freshness: daily (rebuilt from INSEE Sirene + INPI RNE)
- Record shape: `{ results: [company], total_results, page, per_page }`
- Primary keys: `siren`
- Join keys: `siren`, `siege.siret`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| results[].siren | siren | Company id (9-digit) | string | identifier | `356000000` | universal join key |
| results[].nom_complet | nom_complet | Display name | string | legal_name | `LA POSTE` | |
| results[].nom_raison_sociale | nom_raison_sociale | Legal name | string | legal_name | `LA POSTE` | null for individuals |
| results[].nature_juridique | nature_juridique | Legal form code | string | legal_form | `5510` | INSEE cat. juridique |
| results[].activite_principale | activite_principale | NAF Rev2 / APE | string | activity | `53.10Z` | |
| results[].activite_principale_naf25 | activite_principale_naf25 | NAF2025 | string | activity | `53.10Y` | keep both |
| results[].etat_administratif | etat_administratif | A=active / C=ceased | string | status | `A` | |
| results[].date_creation | date_creation | Creation date | date | date | `1991-01-01` | incorporation |
| results[].date_fermeture | date_fermeture | Closure date | date | date | — | dissolution |
| results[].categorie_entreprise | categorie_entreprise | PME/ETI/GE | string | metadata | `GE` | size category |
| results[].tranche_effectif_salarie | tranche_effectif_salarie | Employee band code | string | employment | `53` | band, not exact |
| results[].statut_diffusion | statut_diffusion | O/P diffusion | string | metadata | `O` | P → mask PII |
| results[].dirigeants[] | dirigeants | Directors | array | person | `{nom,prenoms,annee_de_naissance,qualite,type_dirigeant}` | **PII** |
| **results[].finances** | finances | Per-year financials | object | financial | `{"2024":{"ca":34569000000,"resultat_net":1722000000}}` | **open, no auth** |
| results[].finances.<y>.ca | ca | Revenue (EUR) | integer | financial | `34569000000` | chiffre d'affaires |
| results[].finances.<y>.resultat_net | resultat_net | Net income (EUR) | integer | financial | `1722000000` | neg = loss |
| results[].siege | siege | Head-office establishment | object | address | — | siret + address + geo |
| results[].siege.siret | siret | HQ establishment id (14) | string | identifier | — | SIREN+NIC |
| results[].nombre_etablissements | nombre_etablissements | # establishments | integer | metadata | — | |
| results[].complements | complements | Boolean flags | object | metadata | — | est_association/est_ess/... |

## Interpretation Notes

- **The fastest open entry point** — no auth, one call returns identity + activity + status + HQ address
  + geo + directors + **headline financials**. It merges INSEE Sirene + INPI RNE, so it overlaps the
  spine sources but is convenient for lookups/enrichment (not a bulk dump path — use Sirene/INPI bulk for
  the full population).
- **`finances` is the open-financials highlight**: a per-year object with `ca` (revenue) and
  `resultat_net` (net income). Only these two figures, only for years with a **non-confidential** INPI
  filing; `null`/absent otherwise. Verified live: `{"2024":{"ca":34569000000,"resultat_net":1722000000}}`.
- **Codes need label tables**: `nature_juridique` (INSEE catégorie juridique), `tranche_effectif_salarie`
  (employee band), NAF Rev2 vs NAF2025 — keep mapping tables.
- **PII**: `dirigeants[]` carries names + birth year/month; `statut_diffusion=P` and
  `complements.est_entrepreneur_individuel=true` mark privacy-sensitive natural persons → GDPR.
- See `sample_record.json` for one real (trimmed) record from `raw/api/recherche_entreprises_finances_sample.json`.
