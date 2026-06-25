# Search attempts — Morocco

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `ompic.ma`, `directinfo.ma`, `registreducommerce.ma`,
  `casablanca-bourse.com`, `data.gov.ma`
- Language: French, Arabic
- Result: ompic.ma 000 (timeout); directinfo.ma 200; casablanca-bourse 307→200;
  data.gov.ma 200
- Decision: pursue directinfo (OMPIC), Casablanca Bourse, data.gov.ma

## Attempt 2
- Date/time: 2026-06-25
- Source: directinfo.ma (OMPIC)
- Query: parse home (search, ICE, registre, bilans, API, tarif)
- Result: free "Recherche avancée" but **reCAPTCHA-gated** (recaptcha/api.js); paid
  detailed data + Bilans (financials) + documented OMPIC API (CAPI)
- Decision: OMPIC = reCAPTCHA + paid; no open bulk/API

## Attempt 3
- Date/time: 2026-06-25
- Source: data.gov.ma (CKAN)
- Query: package_search q=entreprise / registre commerce / ompic / societe
- Result: working CKAN, but **no company register** — only statistics (Bank
  Al-Maghrib, CNSS, ANRT) and unrelated datasets (registre/ompic = 0)
- Decision: data.gov.ma = no company dataset (statistics)

## Attempt 4
- Date/time: 2026-06-25
- Source: Casablanca Stock Exchange (`casablanca-bourse.com`)
- Query: `/fr/listing-des-emetteurs`, `/fr/publications-des-emetteurs`
- Result: **OPEN** — issuer listing returns real listed companies (AFMA SA, Afric
  Industries SA, Alliances Développement Immobilier SA, Atlanta Sanad, banks,
  holdings); issuer publications (financials) HTTP 200
- Decision: Casablanca Bourse = recommended (open, listed)

## Attempt 5
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: ICE, RC, IF, Patente, CNSS
- Result: ICE (15-digit unified id), RC (per-court), IF (tax, DGI), Patente, CNSS
- Decision: document identifier model (ICE is the unified key)
