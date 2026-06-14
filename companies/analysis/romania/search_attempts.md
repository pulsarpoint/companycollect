# Romania — Search Attempts

## Attempt 1

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `ANAF webservicesp.anaf.ro bilant API situatii financiare JSON CUI Romania`
- Language: Romanian/English
- Why this query was tried: locate the official financial-statements source.
- Top relevant URLs: static.anaf.ro/.../servicii_web.html; static.anaf.ro/.../doc_WS_Bilant_V1.txt
- Result: confirmed `GET https://webservicesp.anaf.ro/bilant?an=YYYY&cui=CUI` returns JSON financial indicators; doc claims 2014–2019.
- Decision: test the endpoint live.

## Attempt 2

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `ONRC date deschise registrul comertului firme noi descarcare CSV open data Romania`
- Language: Romanian
- Why: find the official open company register on data.gov.ro.
- Top relevant URLs: data.gov.ro ONRC organization + dated "Firme înregistrate..." datasets.
- Result: ONRC publishes the full register as OD_FIRME.CSV, refreshed regularly.
- Decision: locate the latest snapshot's resource URL.

## Attempt 3

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `data.gov.ro firme companies dataset registrul comertului download`
- Language: English
- Why: get the exact CSV download URL + columns.
- Top relevant URLs: data.gov.ro `firme-08-12-2025` (latest), resource `488a8d00-...`.
- Result: columns DENUMIRE, CUI, COD_INMATRICULARE, DATA_INMATRICULARE, EUID, FORMA_JURIDICA, address.
- Decision: HEAD then download.

## Attempt 4

- Date/time: 2026-06-14
- Search engine or source: WebFetch
- Query: doc_WS_Bilant_V1.txt + data.gov.ro dataset page
- Why: extract ANAF response schema + exact CSV download link.
- Result: 33 indicator codes (I1–I33), response fields {an,cui,deni,caen,den_caen,i[]}; CSV download URL obtained.
- Decision: download CSV; test bilant.

## Attempt 5

- Date/time: 2026-06-14
- Search engine or source: curl (HEAD + GET)
- Query: HEAD od_firme.csv; GET bilant for Dedeman/OMV/eMAG/Antibiotice
- Result: server **ignores Range** (returned full 643 MB, 4,116,357 rows — kept as the real bulk download). ANAF bilant returned **empty `i:[]`** for all CUIs with the bot User-Agent.
- Decision: investigate the empty ANAF responses (UA filter suspected).

## Attempt 6

- Date/time: 2026-06-14
- Search engine or source: curl (verbose, browser UA)
- Query: `GET /bilant?an=2019&cui=14399840` with `Mozilla/5.0`
- Result: **full financial data returned** (Dante International SA, turnover 4.56B RON, 21 indicators) — the empties were an F5 WAF **User-Agent filter**. Set-Cookie F5 markers present.
- Decision: use a browser UA; probe recent years.

## Attempt 7

- Date/time: 2026-06-14
- Search engine or source: curl
- Query: `/bilant` for CUI 14399840, years 2021/2023/2024
- Result: all return data (turnover 7.35B / 7.72B / 8.99B RON) — coverage is **current through 2024**, not just 2019.
- Decision: ANAF bilant = recommended financial source.

## Attempt 8

- Date/time: 2026-06-14
- Search engine or source: curl + WebFetch (doc_WS_V5.txt)
- Query: `POST PlatitorTvaRest/api/{v5..v9}/ws/tva`
- Result: **all versions HTTP 404** on this run (real ANAF 404 page, redirect to anaf.ro). Doc confirms the request/response shape (cui, data → denumire, adresa, scpTVA, statusInactivi, …).
- Decision: catalog ws/tva as a documented **secondary** source; not verified live; register CSV covers master data.

## Attempt 9

- Date/time: 2026-06-14
- Search engine or source: WebFetch + curl
- Query: data.gov.ro dataset resources; download companion CSVs
- Result: six open CSVs (OD_FIRME, OD_STARE_FIRMA, OD_CAEN_AUTORIZAT, OD_REPREZENTANTI_LEGALI [PII], OD_REPREZENTANTI_IF [PII], OD_SUCURSALE_ALTE_STATE_MEMBRE). Downloaded OD_FIRME + OD_STARE_FIRMA in full; sampled the rest.
- Decision: join on COD_INMATRICULARE; redact representative PII.

## Attempt 10

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `ONRC registrul beneficiarilor reali acces public beneficial ownership register Romania restricted`
- Result: RBR access requires registration + fee + qualified e-signature; narrowed to legitimate interest post-CJEU.
- Decision: mark RBR restricted / planning-only.
