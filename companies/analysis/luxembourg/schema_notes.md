# Luxembourg — Schema Notes

No per-company open record was lawfully downloadable in bulk (RCS search captcha-gated; no open bulk/API). Fields
below are documented from the RCS/LBR data model and the comptes annuels (eCDF). Join on the **RCS number** and
the **matricule** across sources.

## Identifiers
- **RCS number** — register identifier; the prefix encodes the entity class:
  - **B** — sociétés (companies) — the common case (e.g. `B123456`).
  - **A** — personnes physiques (sole traders / commerçants).
  - **F** — succursales (branches).
  - (others for associations, civil-law entities, etc.).
- **Matricule** (numéro d'identification national) — 13 digits (e.g. `20152411234`); the national id, also used
  tax-side.
- **VAT** — `LU` + 8 digits; separate from RCS/matricule (VIES/AED).
- Names/forms are mainly in **French** (Luxembourg administrative language; also German).

## RCS company record — documented fields
```
rcs_number          - RCS number (B...)
matricule           - 13-digit national identification number
denomination        - dénomination / nom (legal name)
forme_juridique     - legal form (S.A., S.à r.l., SCA, SCS, SCSp, succursale, ...)
siege_social        - registered office address
statut              - status (inscrite/active, en liquidation, radiée/struck off, ...)
date_constitution   - incorporation date
representants       - directors/managers (gérants/administrateurs) [PII; in documents]
documents           - filed documents (statuts, comptes annuels, résolutions)
```

## Annual accounts (comptes annuels / eCDF) — document-based
```
bilan (balance sheet): actif (assets), passif (liabilities + capitaux propres/equity)
compte de profits et pertes (P&L): chiffre d'affaires (turnover), résultat (result)
annexes (notes)
filing format: eCDF (structured) ; published as PDF on the RCS page ; abridged for small companies ; EUR
```
- Free to download per company; NOT open structured bulk. Structured figures via OCR/parse or a commercial
  provider. Join on RCS number.

## Mapping to internal company model
```
company_id          <- rcs_number (B...) ; cross-key matricule
registration_number <- rcs_number
rcs_number          <- RCS number
matricule           <- 13-digit national id
tax_id              <- matricule (national id) ; VAT separate
vat_id              <- LU + 8 digits (VIES/AED; not the matricule)
legal_name          <- denomination
company_type        <- forme_juridique (S.A./S.à r.l./SCSp/...)
status              <- statut (inscrite/en liquidation/radiée)
incorporation_date  <- date_constitution
registered_address  <- siege_social
municipality        <- from address (commune)
activity_code       <- not_available (no public NACE in the free RCS data)
officers[]          <- representants (gérants/administrateurs) [PII; in documents]
financials[]        <- comptes annuels (bilan + P&L; eCDF/PDF; parse) | commercial provider [EUR]
beneficial_owners[] <- RBE (restricted) [PII]
country             <- "Luxembourg"
source_url/name/at, raw_record
```
See `companies/data/luxembourg/normalized/companies.sample.jsonl` (schematic — no per-company open record was
lawfully downloadable in bulk here).
