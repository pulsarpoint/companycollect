# Czech Republic — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: ARES REST API (direct, live)
- Query: `GET /ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/00006947` then `/27082440`
- Language: Czech (API)
- Why this query was tried: Confirm ARES is a live open REST API and inspect the per-subject JSON schema.
- Top relevant URLs:
  - https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/00006947
  - https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/27082440
- Result: HTTP 200 both. Rich JSON: ico, obchodniJmeno, dic (CZ+IČO), pravniForma, structured sidlo (obec/okres/kraj codes), czNace2008, seznamRegistraci (per-register status), datumAktualizace. IČO 27082440 = Alza.cz a.s.
- Decision: ARES API = recommended API spine.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: ARES search endpoint (live POST)
- Query: `POST /ekonomicke-subjekty/vyhledat {"obchodniJmeno":"Alza","start":0,"pocet":3}`
- Language: Czech
- Why this query was tried: Confirm search + pagination shape for full-population or name lookups.
- Top relevant URLs:
  - https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/vyhledat
- Result: HTTP 200; envelope `{pocetCelkem, ekonomickeSubjekty[]}`. Returned Alza.cz a.s. (27082440) + MS - alza, s.r.o.
- Decision: Paging via start/pocet against pocetCelkem.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `ARES ekonomické subjekty open data bulk export hromadná data justice.cz sbírka listin účetní závěrka`
- Language: Czech
- Why this query was tried: Find official bulk exports and the financial-statements (Sbírka listin) path.
- Top relevant URLs:
  - https://ares.gov.cz/stranky/otevrena-data
  - https://data.mf.gov.cz/topics/ares
  - https://dataor.justice.cz/
- Result: ARES has an open-data bulk export of the commercial register (MF portal). Justice publishes the public register as open data at dataor.justice.cz. Financial statements live in the Sbírka listin.
- Decision: Probe dataor.justice.cz (CKAN) and the ARES open-data bulk; treat Sbírka listin as the financials source (PDF).

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — dataor.justice.cz CKAN API
- Query: `GET /api/3/action/package_list`, `package_show?id=as-actual-praha-2026`
- Result: 9,496 packages named `{legalform}-{full|actual}-{court}-{year}` (sro, as, pobspolek, sf, …). Package has 4 resources: csv, xml, csv.gz, xml.gz.
- Decision: This is the deep open register bulk. Download an a.s. actual dump to inspect the schema.

## Attempt 5
- Date/time: 2026-06-14
- Source: curl -L (live) — Justice bulk file
- Query: `GET http://dataor.justice.cz/api/file/as-actual-praha-2026.xml.gz`
- Result: First attempt without -L → HTTP 302, 0 bytes. With -L → HTTP 200, 15,248,722 bytes (~192 MB uncompressed, ~16,758 a.s.). Schema = `<Subjekt>` + typed `<Udaj>` items keyed by udajTyp/kod. Confirmed share capital, officers (with DOB), shareholders (AKCIONAR), supervisory board, insolvency.
- Decision: Downloaded + saved with SHA-256 metadata. Extracted a real `<Subjekt>` sample (CR Holding a.s.) and built the normalized sample.

## Attempt 6
- Date/time: 2026-06-14
- Source: curl (live) — or.justice.cz public register / Sbírka listin
- Query: `GET /ias/ui/rejstrik`, `/ias/ui/vypis-sl-firma?subjektId=`
- Result: HTTP 200. Financial statements (účetní závěrka) are free to view in the Sbírka listin as PDFs; no structured/XBRL bulk.
- Decision: Sbírka listin = useful_secondary (financials, document-based PDF).
