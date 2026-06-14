# Czech Republic — Company Open Data Investigation

## Conclusion

The Czech Republic is a **fully-open** country for company identity and structure, and **partially open** for
financials. Everything joins on the **IČO** (Identifikační číslo osoby, 8 digits). Two official open sources
dominate, and both were verified live during this investigation:

- **ARES** (Administrativní registr ekonomických subjektů), Ministry of Finance — a modern REST API that
  aggregates the underlying registers (ROS, RES/ČSÚ, VR/Justice, RŽP, DPH). One `GET /ekonomicke-subjekty/{ico}`
  returns rich JSON; `POST /ekonomicke-subjekty/vyhledat` searches with paging.
- **Veřejný rejstřík** (public register), Ministry of Justice — published as **open bulk data** on the CKAN
  portal `dataor.justice.cz` as full and "actual" (Platný výpis) XML/CSV dumps, segmented by legal form, court
  region and year. This is the deepest structured source (officers, shareholders, share capital, boards,
  insolvency).

## What was verified (live)

- `GET https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/00006947` → HTTP 200, rich JSON
  (Ministerstvo financí). Then IČO `27082440` → **Alza.cz a.s.**, DIČ `CZ27082440`, legal form `121`
  (a.s.), CZ-NACE list, structured sídlo.
- `POST .../vyhledat` with `{"obchodniJmeno":"Alza","pocet":3}` → HTTP 200, `pocetCelkem` + `ekonomickeSubjekty[]`
  paging envelope.
- `https://dataor.justice.cz/api/3/action/package_list` → **9,496 packages** named
  `{sro|as|...}-{full|actual}-{court}-{year}` (e.g. `sro-full-praha-2026`, `as-actual-brno-2026`).
- `package_show?id=as-actual-praha-2026` → 4 resources (csv, xml, csv.gz, xml.gz).
- Downloaded `as-actual-praha-2026.xml.gz` (**15,248,722 bytes**, ~192 MB uncompressed, **~16,758 a.s.**;
  links 302-redirect → use `-L`). Inspected the schema: `<Subjekt>` with `nazev`, `ico`, `zapisDatum`, and a
  list of typed `<Udaj>` records keyed by `udajTyp/kod`.
- `or.justice.cz` public register + Sbírka listin pages → HTTP 200 (financial statements free to view, PDF).

## Register schema (Justice VR XML)

Each company is a `<Subjekt>` (nazev, ico, zapisDatum) plus a list of typed data items `<Udaj>` whose
`udajTyp/kod` is the dictionary. Most frequent codes in the a.s. dump:

```
PREDMET_PODNIKANI / PREDMET_CINNOSTI  - scope of business / activity (free text)
STATUTARNI_ORGAN(_CLEN)               - board / statutory body members (osoba: jmeno, prijmeni, narozDatum=DOB) [PII]
AKCIE / AKCIONAR                      - shares / SHAREHOLDERS (present for a.s.)
DOZORCI_RADA(_CLEN)                   - supervisory board [PII]
ZAKLADNI_KAPITAL                      - share/registered capital (KORUNY + % paid)
SIDLO                                 - registered office (structured: obec, castObce, ulice, psc, okres)
PRAVNI_FORMA                          - legal form (kod/nazev/zkratka)
SPIS_ZN                               - court file mark (soud/oddil/vlozka)
LIKVIDATOR / INSOLVENCE               - liquidation / insolvency
```

Real example record extracted: **CR Holding a.s.**, IČO 3431509, registered 2014-09-24, share capital
183,800,000 CZK, Washingtonova 1624/5, Nové Město, 11000 Praha. Saved at
`data/.../raw/samples/justice_subjekt_first.xml` and normalized to `normalized/companies.sample.jsonl`.

## Identifiers

- **IČO** — 8-digit company id and the universal join key (note: leading zeros; the register may store it
  without padding, e.g. `3431509` = `03431509`).
- **DIČ** — tax/VAT id, normally `CZ` + IČO (e.g. `CZ27082440`). Confirm/validate via the Registr DPH / VIES.
- **spisová značka** — court file mark (soud + oddíl + vložka), e.g. `B 20032/MSPH`.

## Financial data

- **Účetní závěrka** (financial statements: rozvaha / výkaz zisku a ztráty) and výroční zpráva are filed into
  the **Sbírka listin** and are **free to view** at `or.justice.cz` — but as **PDF documents**, not structured
  open data. There is **no official XBRL/CSV** bulk of figures.
- The register dump *does* carry **share capital** (ZAKLADNI_KAPITAL) as a structured value, but not the
  annual statements.
- Structured financials at scale therefore need OCR/parsing of Sbírka listin PDFs or a commercial provider.

## Recommended ingestion

Hybrid: ingest the Justice `*-actual-*` XML dumps (all legal forms × court regions) for the deep register,
keyed on IČO; enrich/refresh per-IČO via the ARES API (respect the ~tens-of-thousands/day limit). Pull CZ-NACE
/ sector / size from ČSÚ RES (also via ARES). Treat financials as a separate document-fetch from Sbírka listin.

## Risks / open questions

- **License exactness**: the CKAN package `license_id` is empty; confirm the Justice VR reuse terms and the
  ARES open-data terms before redistribution.
- **GDPR**: officers and (for a.s.) shareholders include **date of birth** — personal data; apply a lawful
  basis + retention, no direct-marketing reuse.
- **IČO zero-padding** must be normalized consistently across ARES (padded) and Justice (sometimes unpadded).
- **Financials** are not structured open data (PDF only).
