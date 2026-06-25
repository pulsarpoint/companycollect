# Casablanca Stock Exchange (listed companies + financials) Field Catalog

## Source Summary

- Country: Morocco
- Source type: financial_disclosure
- Organization: Bourse de Casablanca
- URL: https://www.casablanca-bourse.com/fr/listing-des-emetteurs
- License: public disclosure
- Access: **public, open** (HTML)
- Freshness: event-driven / quarterly
- Record shape: HTML issuer directory, one row per listed company
- Primary keys: ticker
- Join keys: ticker, isin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Émetteur / Raison sociale | Listed company name | string | legal_name | AFMA SA | verified on page |
| ticker | Ticker / Code valeur | Casablanca ticker | string | identifier | AFMA, ADI, ATL | listed key |
| sector | Secteur | Bourse sector | string | activity | Assurances, Immobilier | |
| isin | ISIN | Securities id | string | identifier | MA... | Moroccan ISINs start MA |
| financial_publications | Publications des émetteurs | Issuer financials | array | financial |  | MAD; listed only |

## Interpretation Notes

- **Casablanca Stock Exchange** is the one **open** Moroccan company source. The
  issuer listing (`/fr/listing-des-emetteurs`) lists all listed companies (name,
  sector) — **verified live**: AFMA SA, Afric Industries SA, Alliances Développement
  Immobilier SA, Atlanta Sanad, plus banks and holdings sections (and well-known blue
  chips: Attijariwafa Bank, Maroc Telecom, BCP, Cosumar).
- **Issuer publications / financial results** are at `/fr/publications-des-emetteurs`
  (financial statements, MAD). The site also has instruments/actions pages.
- **Scope**: listed companies only (~75). Join to OMPIC by company name / ICE (the
  ICE / RC / IF are not on the Bourse — those are OMPIC, paid/reCAPTCHA).
- Pages are HTML (French); parse the directory table. No personal data at the
  directory level. Currency **MAD**.
