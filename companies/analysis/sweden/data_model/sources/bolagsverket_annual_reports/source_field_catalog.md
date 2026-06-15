# Bolagsverket Värdefulla datamängder — Annual Reports (iXBRL) Field Catalog

## Source Summary

- Country: Sweden
- Source type: official_registry_api (document endpoints, OAuth2)
- Organization: Bolagsverket (Swedish Companies Registration Office)
- URL: https://gw.api.bolagsverket.se/vardefulla-datamangder/v1/dokumentlista (+ GET /dokument/{id})
- License: Free of charge, no contract — EU high-value datasets. **Document-file reuse wording may be specific — verify** per `license_notes.md`.
- Access: public, OAuth2 client_credentials gated (same credentials as `bolagsverket_vdm`)
- Freshness: real-time as filings arrive; reported per fiscal year
- Record shape: `POST /dokumentlista` (by orgnr) → document list; `GET /dokument/{id}` → **ZIP** containing one **iXBRL** annual report
- Primary keys: organisationsnummer + dokument_id + räkenskapsår
- Join keys: organisationsnummer

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| dokumentlista[].dokumentId | dokument id | Document id to download | string | document | — | Pick latest annual report/year |
| dokumentlista[].dokumenttyp | document type | e.g. årsredovisning | string | document | årsredovisning | Filter to annual reports |
| dokumentlista[].rakenskapsar | räkenskapsår | Financial year of doc | string | filing | — | Year/period unconfirmed shape |
| ixbrl:Rakenskapsar | Räkenskapsår | Reporting period (start/end) | object | date | — | From XBRL context |
| ixbrl:Valuta | valuta | Reporting currency | string | financial | SEK | ISO 4217; from XBRL unit |
| ixbrl:Regelverk | regelverk | K2/K3 framework | string | financial | K2, K3 | Drives concept availability |
| ixbrl:Nettoomsattning | Nettoomsättning | Net revenue | decimal | financial | — | May be omitted under abbreviated K2 |
| ixbrl:Rorelseresultat | Rörelseresultat | Operating profit (EBIT) | decimal | financial | — | — |
| ixbrl:ResultatEfterFinansiellaPoster | Resultat efter finansiella poster | Profit after financial items | decimal | financial | — | — |
| ixbrl:AretsResultat | Årets resultat | Net profit for year | decimal | financial | — | Bottom line |
| ixbrl:SummaTillgangar | Summa tillgångar | Total assets | decimal | financial | — | — |
| ixbrl:SummaEgetKapital | Summa eget kapital | Total equity | decimal | financial | — | Derive soliditet downstream |
| ixbrl:SummaSkulder | Summa skulder | Total liabilities | decimal | financial | — | May sum kort-/långfristiga |
| ixbrl:Anlaggningstillgangar | Anläggningstillgångar / Omsättningstillgångar | Fixed & current assets | decimal | financial | — | Optional detail |
| ixbrl:Revisor | revisor | Audited / auditor present | boolean | financial | — | Many small AB are audit-exempt |
| ixbrl:_taxonomy_concept_names | (raw XBRL element names) | Raw XBRL concepts/contexts/units | object | raw_extension | — | Keep raw mapping |

## Interpretation Notes

- **No authenticated document was retrieved** (OAuth-gated). The line items above are the **standard
  Swedish K2/K3 annual-report concepts** (income statement + balance sheet) named in
  `schema_notes.md` and published at `taxonomier.se`. The Swedish labels **identify a concept, not a
  literal JSON key** — you must parse the real iXBRL instance with an XBRL parser keyed to the K2/K3
  taxonomy and resolve the actual element names, contexts (period), units (currency), and decimals.
- **Two-step retrieval.** `/dokumentlista` enumerates available documents per orgnr; `/dokument/{id}`
  returns a **ZIP** that contains the report as **inline XBRL (iXBRL)**. Unzip, then parse the iXBRL.
- **K2 vs K3.** The framework (`regelverk`) determines which concepts exist. Small companies often
  file abbreviated K2 reports that omit revenue and detailed sub-totals — treat missing figures as
  "not disclosed", not zero.
- **No pre-computed ratios.** Derive ratios (e.g. soliditet = equity / assets) downstream from the
  parsed figures.
- **Coverage gap.** Free iXBRL exists only for companies that filed **digitally**. Older paper-only
  filings will not appear; quantify historical gaps and do not assume full back-history.
- **Currency.** Usually SEK but confirm via the XBRL unit; some entities report in EUR/USD.
- **Persist raw.** Keep the raw parsed iXBRL→concept map alongside normalized figures so
  re-normalization across taxonomy-version changes does not require re-downloading.

No `sample_record.json` is provided: the source is OAuth-gated, no document was retrieved, and the
real artifact is a multi-file iXBRL package — fabricating one would misrepresent the structure.
