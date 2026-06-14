# Veřejný rejstřík — Justice open data (dataor.justice.cz) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: official_registry
- Organization: Ministerstvo spravedlnosti ČR (Ministry of Justice)
- URL: https://dataor.justice.cz/ (CKAN; files https://dataor.justice.cz/api/file/{package}.xml.gz — **302 → use -L**)
- License: Open data (CKAN package license field **empty** — confirm exact terms with MSp)
- Access: public
- Freshness: monthly full + frequent incremental; packages `{sro|as|pobspolek|sf|...}-{full|actual}-{court}-{year}`
- Record shape: XML `<Subjekt>` (nazev, ico, zapisDatum) + `<udaje><Udaj>` typed items keyed by `udajTyp/kod`
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Subjekt.ico | ico | IČO company id | string | identifier | 3431509 | unpadded — normalize |
| Subjekt.nazev | nazev | Name | string | legal_name | CR Holding a.s. | |
| Subjekt.zapisDatum | zapisDatum | Registration date | date | date | 2014-09-24 | incorporation |
| Udaj[PRAVNI_FORMA] | PRAVNI_FORMA | Legal form | string | legal_form | Akciová společnost / as | code+name+abbr |
| Udaj[SIDLO].adresa | SIDLO | Registered office | object | address | Washingtonova 1624/5, … Praha | text only (no RUIAN) |
| Udaj[ZAKLADNI_KAPITAL] | ZAKLADNI_KAPITAL | Share capital (CZK) | decimal | financial | 183800000 | + % paid |
| Udaj[PREDMET_PODNIKANI] | PREDMET_PODNIKANI | Scope of business | string | activity | (free text) | NOT a NACE code |
| Udaj[STATUTARNI_ORGAN_CLEN] | STATUTARNI_ORGAN_CLEN | Board member | object | person | funkce: předseda | **DOB → PII** |
| Udaj[DOZORCI_RADA_CLEN] | DOZORCI_RADA_CLEN | Supervisory board member | object | person | — | **DOB → PII** |
| Udaj[AKCIONAR] | AKCIONAR | Shareholder (a.s.) | object | ownership | — | s.r.o. → SPOLECNIK |
| Udaj[SPIS_ZN].spisZn | SPIS_ZN | Court file mark | object | identifier | B 20032/MSPH | Sbírka listin link |
| Udaj[ZAKLADNI_KAPITAL].splaceni | splaceni | Capital paid % | decimal | financial | 100 | |
| Udaj[INSOLVENCE] | INSOLVENCE | Insolvency | object | status | — | → status |
| Udaj[LIKVIDATOR] | LIKVIDATOR | Liquidator | object | person | — | → status (liquidation), PII |

## Interpretation Notes

- **Deepest open register.** Unlike ARES, the Justice dump exposes governance and ownership: **board members**,
  **supervisory board**, **shareholders (AKCIONAR for a.s. / SPOLECNIK for s.r.o.)**, **share capital**,
  liquidation and insolvency — all as typed `<Udaj>` items keyed by `udajTyp/kod`.
- **Packages.** Named `{legalform}-{full|actual}-{court}-{year}`. `full` = all historical records;
  `actual` (Platný výpis) = the current valid extract. Ingest the `actual` set for current state; use `full`
  for history. Court regions: praha, brno, ostrava, plzen, ceske_budejovice, hradec_kralove, … Legal forms:
  sro, as, pobspolek (branch/association), sf (foundation funds), etc.
- **IČO padding** differs from ARES (unpadded here) — normalize to 8 digits to join.
- **No RUIAN codes** in the address (text components only); use ARES for geocoding codes.
- **Activity is free text** (PREDMET_PODNIKANI/PREDMET_CINNOSTI), not NACE — take codes from ARES czNace2008.
- **GDPR.** Board, supervisory-board members, liquidators and natural-person shareholders carry **date of
  birth** (narozDatum). Personal data — lawful basis + retention required; no direct-marketing reuse. Example
  values in the catalog are generic (no real person copied).
- **Transport gotcha:** file URLs **302-redirect** — fetch with redirect following (`-L`).
- A real `sample_record.json` (CR Holding a.s., IČO 3431509) is included from the downloaded dump.
