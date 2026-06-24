# Search attempts — Bosnia and Herzegovina

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: HTTP HEAD/GET to `bizreg.pravosudje.ba`, `apif.net`, `bifx.apif.net`,
  `fia.ba`, `uino.gov.ba`
- Language: Bosnian/Serbian/Croatian (BHS)
- Why: locate the entity court registers + financial/tax agencies
- Result: bizreg.pravosudje.ba 200 ("Registar poslovnih subjekata"); apif.net 200;
  bifx.apif.net unreachable; fia.ba 200; uino.gov.ba 200
- Decision: pursue APIF (RS), bizreg.pravosudje.ba (FBiH/Brčko), FIA, UINO

## Attempt 2
- Date/time: 2026-06-24
- Source: APIF home page
- Query: parse registry links (registar poslovnih subjekata, registar finansijskih
  izvještaja, registar boniteta)
- Language: Serbian (Cyrillic/Latin)
- Why: find the RS business + financial registers and their portals
- Result: RS business register search lives at `bizreg.esrpska.com/Home/PretragaPoslovnogSubjekta`;
  RFI (financial statements) and Registar boniteta documented as APIF services
- Decision: probe the RS search portal for an API

## Attempt 3
- Date/time: 2026-06-24
- Source: `bizreg.esrpska.com` RS register
- Query: inspect search page JS → `POST /Home/SearchPoslovniSubjekt` with `term=`;
  ran `term=NOVA BANKA`, `ELEKTROPRIVREDA`, `TELEKOM`
- Language: BHS
- Why: confirm a real, queryable structured endpoint
- Result: **JSON** responses with JIB/MBS/MB/name/address/activity/status/founders.
  Verified Nova banka (JIB 4400374890002), RiTE Gacko (4401387900003), B2 LINK
  (4402978800004). Per-company PDF extract at `/Home/DetaljiPoslovnogSubjekta/{id}`.
- Decision: RECOMMENDED per-company source; no bulk

## Attempt 4
- Date/time: 2026-06-24
- Source: `bizreg.pravosudje.ba` (FBiH/Brčko APEX)
- Query: GET app 183 search pages
- Language: BHS
- Why: cover FBiH + Brčko entities
- Result: Oracle APEX search portal; session-bound; no open JSON/bulk
- Decision: useful per-company secondary source (FBiH/Brčko)

## Attempt 5
- Date/time: 2026-06-24
- Source: APIF RFI article + FIA + UINO
- Query: register of financial statements (RS/FBiH); VAT/PDV
- Language: BHS
- Why: locate financial data and VAT identifiers
- Result: APIF RFI (RS) and FIA (FBiH) hold annual statements (bilans stanja/uspjeha),
  per-company paid; UINO assigns PDV broj (VAT, 12-digit), per-company lookup. No bulk.
- Decision: catalog financials as paid/per-company; document JIB vs PDV broj

## Attempt 6
- Date/time: 2026-06-24
- Source: `data.gov.ba`
- Query: national open-data portal
- Result: did not resolve (no working national open-data portal)
- Decision: record as unavailable
