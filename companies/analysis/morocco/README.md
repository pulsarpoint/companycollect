# Company data sources for Morocco

## Status

- Official bulk data: **not open** — OMPIC has no open bulk register
- Official API: **paid** — OMPIC offers a subscription API (directinfo / CAPI); the
  free search is reCAPTCHA-gated
- Open data portal: `data.gov.ma` is a working CKAN portal but hosts **no company
  register** (statistics only)
- License: OMPIC data is paid/restricted; Casablanca Bourse listed data is public
- Recommended ingestion path: **Casablanca Bourse** for listed companies (open) +
  paid OMPIC/directinfo for the rest

## Best source

The official company registry is **OMPIC — Office Marocain de la Propriété
Industrielle et Commerciale**, which runs the **Registre Central du Commerce (RCC)**.
Company data is served through **`directinfo.ma`**:

- a **free basic search** (company / ICE / trademark) — but **Google
  reCAPTCHA-gated**;
- **paid detailed data** — company profile, **Bilans** (financial statements),
  statistics;
- a **subscription API** (documented; OMPIC CAPI).

`www.ompic.ma` timed out at investigation time. There is **no open bulk register**.
The one genuinely **open** source is the **Casablanca Stock Exchange**.

## Financial data

**Casablanca Stock Exchange** (`casablanca-bourse.com`) — **open**: the issuer
directory (`/fr/listing-des-emetteurs`) and **issuer publications / financial
results** (`/fr/publications-des-emetteurs`). **Verified live**: real listed issuers
incl. **AFMA SA**, **Afric Industries SA**, **Alliances Développement Immobilier
SA**, **Atlanta Sanad**. **Private-company financials (Bilans)** are sold by OMPIC via
directinfo — **not open**. Currency **MAD**.

## Identifiers & tax

- **ICE — Identifiant Commun de l'Entreprise** — the **15-digit unified company id**
  (links registry, tax, social security).
- **RC — Registre du Commerce number** — commercial registration (per court /
  tribunal).
- **IF — Identifiant Fiscal** — tax identifier (DGI).
- **Taxe Professionnelle (Patente)** and **CNSS** (social security) numbers.
- Currency **MAD**. Languages: French + Arabic.

## Next action

Use **Casablanca Bourse** (open) for listed-company directory + financials; use
paid **OMPIC/directinfo** (subscription API or per-company, reCAPTCHA search) for the
rest. There is **no open bulk register and no open private financials**. Managers/
shareholders are personal data (Law 09-08) — redact if obtained.
