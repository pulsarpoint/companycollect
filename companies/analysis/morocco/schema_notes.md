# Schema notes — Morocco

## Identifiers

| Field | Description |
|---|---|
| **ICE — Identifiant Commun de l'Entreprise** | **15-digit** unified company id (links commercial registry, tax, social security). Primary join key. |
| **RC — Registre du Commerce number** | Commercial registration number (per court / tribunal). |
| **IF — Identifiant Fiscal** | Tax identifier (DGI). |
| **Taxe Professionnelle (Patente)** | Local business tax number. |
| **CNSS** | Social security registration number. |

`ICE` is the unified primary key; **RC** is the commercial registration; **IF** the tax id.

## Casablanca Bourse listed-company record (open) — fields

| Field | Meaning | Notes |
|---|---|---|
| company_name | Listed company name | e.g. AFMA SA |
| ticker | Casablanca ticker | e.g. AFMA, ADI, ATL (some public knowledge) |
| sector | Bourse sector | Assurances / Immobilier / Banques / Holdings / etc. |
| isin | ISIN | MA… |
| financial_publications | Issuer financials | MAD |

## OMPIC company record (field model, reCAPTCHA/paid — public knowledge)

| Field (fr) | English | Notes |
|---|---|---|
| Raison sociale | Company name | SA / SARL / branch |
| ICE | Unified company id (15-digit) | |
| Numéro RC | Commercial registration number | per court |
| Identifiant Fiscal (IF) | Tax id | |
| Forme juridique | Legal form | SA/SARL/SARL-AU/SNC/SCS/succursale |
| Statut | Status | en activité / radiée / en liquidation |
| Capital social | Share capital | MAD |
| Activité | Activity / object | |
| Adresse | Registered address | |
| Dirigeants | Managers / directors | **PERSONAL DATA — redact** |
| Associés | Shareholders / partners | **PERSONAL DATA — redact** |
| Bilans | Financial statements | MAD (paid) |

## Legal forms (forme juridique)

| Local | English |
|---|---|
| SA (Société Anonyme) | Public/joint-stock company |
| SARL (Société à Responsabilité Limitée) | Limited liability company |
| SARL-AU (à associé unique) | Single-member LLC |
| SNC / SCS | Partnerships |
| Succursale | Branch of a foreign company |

## Status values

`En activité` (active), `En liquidation` (in liquidation), `Radiée` (struck off).

## Internal model mapping

```
company_id          <- ICE (15-digit) [or RC number]
registration_number <- Numéro RC
tax_id              <- Identifiant Fiscal (IF)
vat_id              <- none separate (TVA tied to the IF)
legal_name          <- Raison sociale
company_type        <- Forme juridique (SA/SARL/...)
status              <- Statut (en activité/liquidation/radiée)
registered_address  <- Adresse
activity_code       <- Activité (NMA / nomenclature)
capital             <- Capital social (MAD)
financials          <- Casablanca Bourse (listed) / OMPIC Bilans (paid), MAD
owners/officers     <- Dirigeants / Associés (PERSONAL DATA — redact)
country             <- "Morocco"
```

## Encoding / formats

- UTF-8; French + Arabic. Currency **MAD**. Dates dd/mm/yyyy.
- Only Casablanca Bourse (listed) is open; OMPIC is reCAPTCHA/paid; data.gov.ma has
  no company dataset.
