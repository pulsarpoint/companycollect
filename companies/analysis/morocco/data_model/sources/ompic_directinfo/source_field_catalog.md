# OMPIC — Registre Central du Commerce (directinfo.ma) Field Catalog

## Source Summary

- Country: Morocco
- Source type: official_registry
- Organization: OMPIC (Office Marocain de la Propriété Industrielle et Commerciale)
- URL: https://www.directinfo.ma/
- License: free search (reCAPTCHA) / paid detail + API
- Access: **reCAPTCHA-gated** search; **paid** detail + Bilans + API
- Freshness: live register
- Record shape: per-company record (reCAPTCHA search; paid detail)
- Primary keys: ICE
- Join keys: ICE, RC number, IF, raison sociale

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ice | ICE | 15-digit unified company id | string | identifier |  | primary key |
| rc_number | Numéro RC | Commercial registration number | string | identifier |  | per court |
| if_tax_id | Identifiant Fiscal (IF) | Tax id | string | identifier |  | tax key |
| raison_sociale | Raison sociale | Company name | string | legal_name |  | |
| forme_juridique | Forme juridique | Legal form | string | legal_form | SA/SARL | |
| statut | Statut | Status | string | status |  | en activité/liquidation/radiée |
| capital_social | Capital social | Share capital | decimal | financial |  | MAD |
| activite | Activité | Activity | string | activity |  | NMA |
| adresse | Adresse / Siège social | Registered office | string | address |  | |
| dirigeants | Dirigeants | Managers | array | person |  | PERSONAL DATA — redact; paid |
| bilans | Bilans | Financial statements | array | financial |  | MAD; paid |

## Interpretation Notes

- **OMPIC** runs the **Registre Central du Commerce (RCC)** and serves company data
  via **directinfo.ma**. Its model:
  - a **free basic search** ("Recherche avancée" — entreprise, ICE, marque) — but
    **Google reCAPTCHA-gated** (`recaptcha/api.js?onload=onloadCallback`);
  - **paid** detailed company data, **Bilans** (financial statements), statistics;
  - a documented **subscription API** (OMPIC CAPI).
- The field model above is documented from **public knowledge** of OMPIC company
  records; **no real values copied** (search reCAPTCHA-gated; detail paid).
- **Identifiers**: the **ICE** (15-digit) is the unified company id and the primary
  join key; the **RC number** is the commercial registration (per court); the **IF**
  is the tax id.
- **Capital / Bilans** are in **MAD** (Bilans paid). **Personal data**: dirigeants
  (managers) and associés (shareholders) are personal data under **Law 09-08** —
  redact.
- Implementation is **blocked on payment** (and reCAPTCHA for the free search);
  planning-only.
