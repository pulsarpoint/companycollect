# Company data sources for Poland

## Status

### Company registry data — OPEN
- Official bulk data: **partial** (no single trivial "download all KRS" file, but a free per-entity API +
  a daily VAT-white-list flat file of all NIP-account pairs)
- Official API: **found, free, no auth** — **KRS API** (`api-krs.ms.gov.pl`, OdpisAktualny/OdpisPelny,
  JSON), **Biała lista VAT** (`wl-api.mf.gov.pl`), CEIDG (sole traders), REGON/GUS BIR1 (free key)
- Open data portal: **found** (dane.gov.pl)
- License: **open** (KRS API public/free reuse; white list MF; dane.gov.pl per-dataset)
- Recommended ingestion path: **KRS API** for the company spine + **VAT white list** to bridge
  NIP↔REGON↔KRS + bank accounts; iterate KRS numbers or seed from the white-list flat file

### Financial data (sprawozdania finansowe) — OPEN, structured
- Official bulk data: **per-company free** via **RDF (Repozytorium Dokumentów Finansowych)**
- Official API: free per-company download (XML + PDF); a PRS-eKRS automated API exists (registration)
- Format: **structured XML** (e-Sprawozdania finansowe, MF logical schema — bilans, rachunek zysków i
  strat, informacja dodatkowa); some XBRL for listed/consolidated
- License: open (free access/download)
- Recommended ingestion path: **RDF per-company XML** (free, structured), triggered off the KRS
  "wzmianki o złożonych dokumentach" filing mentions

## Best source

**KRS API** (Krajowy Rejestr Sądowy, Ministry of Justice) is a **free, no-auth, well-structured JSON API**
returning full register data per company (KRS/NIP/REGON, forma prawna, address+website, capital, PKD
activity, board/partners, fiscal year, liquidation/bankruptcy, filed-document mentions). Personal data is
anonymized. **Verified live.** Pair it with the **VAT white list** (free, bridges NIP↔REGON↔KRS + bank
accounts + VAT status) and the **RDF** for **free structured financial statements**. Poland is among the
**most open** countries — comparable to France/Norway, with the bonus of **free machine-readable financials**.

## Next action

1. Build the company spine from the **KRS API** (enumerate KRS numbers / both registers P and S).
2. Use the **VAT white-list daily flat file** (all active NIPs) as a seed + to bridge NIP↔REGON↔KRS.
3. **Financials:** pull free **RDF XML** statements per company; parse the MF e-Sprawozdania schema
   (bilans + rachunek zysków i strat). Add CEIDG for sole traders and CRBR for beneficial ownership.
4. Confirm reuse terms (open, but record attribution) — see `license_notes.md`.

See `investigation.md` for detail and `source_inventory.md` for the table.
