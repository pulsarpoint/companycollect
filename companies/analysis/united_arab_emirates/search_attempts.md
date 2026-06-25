# Search attempts — United Arab Emirates

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `moec.gov.ae`, `ner.economy.gov.ae`, `difc.ae`, `adgm.com`, `dfm.ae`,
  `adx.ae`
- Language: Arabic, English
- Result: moec 302→200 (moet.gov.ae); ner.economy 000; difc 429; adgm 200; dfm 200;
  adx 403
- Decision: pursue NER, DIFC/ADGM registers, DFM/ADX

## Attempt 2
- Date/time: 2026-06-25
- Source: ADGM + MoE + DFM
- Query: ADGM `/public-registers`; MoE home; DFM company pages
- Result: ADGM public-registers page loads but the register search app
  (registration.adgm.com) is 403; MoE is a Liferay portal (no open dataset); DFM
  company pages 301/404 (SPA)
- Decision: free-zone register apps WAF-gated; check exchanges + open data

## Attempt 3
- Date/time: 2026-06-25
- Source: DIFC / ADX / open-data portals / Invest in Dubai
- Query: DIFC public register; ADX issuers; bayanat.ae; data.gov.ae; invest.dubai.ae
- Result: DIFC 429 (WAF/rate-limited); ADX 403; bayanat/data.gov.ae 000 (unreachable);
  invest.dubai.ae 403
- Decision: all gated/unreachable; document as heavily-gated

## Attempt 4
- Date/time: 2026-06-25
- Source: DFM data feed + NER DNS
- Query: DFM home feed endpoints; `host ner.economy.gov.ae` / `economy.gov.ae`
- Result: DFM feeds (connexions.dfm.ae, feeds.dfm.ae) auth-gated; ner.economy.gov.ae
  NXDOMAIN; economy.gov.ae resolves (86.96.x.x, login-gated NER)
- Decision: NER login-only; no open feed

## Attempt 5
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: trade/commercial license number; TRN; economic register number
- Result: trade/commercial license (per emirate/free zone), TRN (15-digit FTA),
  economic register number (NER), free-zone registration numbers
- Decision: document identifier model
