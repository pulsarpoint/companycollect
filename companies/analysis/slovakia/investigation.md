# Slovakia Company Data — Investigation

## Conclusion

Slovakia is **best-in-class fully-open**. Two official, free, machine-readable
registers cover identity and financials, both with permissive licences, joined on
the **IČO** (8-digit company id):

- **RPO — Register právnických osôb, podnikateľov a orgánov verejnej moci**
  (Štatistický úrad SR — Statistics Office), `https://api.statistics.sk/rpo/v1/`.
  The "single public register" that consolidates the **commercial register
  (Obchodný register)**, trade register, association register, etc. Very rich:
  identity + history, legal form, activities, **statutory bodies (officers)**,
  **stakeholders (shareholders)**, **share capital**, predecessors. **CC-BY 4.0**.
- **RÚZ — Register účtovných závierok** (Register of Financial Statements,
  Ministry of Finance / DataCentrum), `https://www.registeruz.sk/cruz-public/api/`.
  Accounting-unit master data **and full structured financial statements**
  (balance sheet + income statement). **CC0** (public domain).

## Identifiers

- **IČO** — 8-digit company identification number; the universal join key
  (present in both RPO and RÚZ).
- **DIČ** — tax identification number (10-digit); in RÚZ accounting units.
- **IČ DPH** — VAT number = `SK` + DIČ for VAT-registered entities.
- RÚZ also exposes internal `id` (accounting-unit id) and `sidlo` (settlement
  code); RPO exposes an internal entity `id`.

## Sources found

### 1. RPO — Register of Legal Entities (api.statistics.sk) — RECOMMENDED
- `GET https://api.statistics.sk/rpo/v1/search?identifier={ICO}` → list of
  matches (`results[]`: id, identifiers, fullNames, addresses, establishment,
  sourceRegister).
- `GET https://api.statistics.sk/rpo/v1/entity/{id}` → full record. Verified for
  ESET (id 937053): `identifiers`, `fullNames` (history), `addresses` (history),
  `legalForms` (code 112 = s.r.o.), `establishment`, `activities[19]`,
  `statutoryBodies[3]` (Konateľ — directors, **with addresses**),
  `stakeholders[9]` (Spoločník — shareholders), `otherLegalFacts`,
  `authorizations` (signing rules), `equities` (share capital, EUR), `deposits`
  (per-person capital contributions), `sourceRegister`, `predecessors`,
  `statisticalCodes` (main SK NACE).
- License **CC-BY 4.0** (returned inline in `license`). A V2 exists; the original
  API is marked deprecated but still serving.
- **Personal data**: statutory bodies, stakeholders, deposits carry person names
  and addresses → redact.

### 2. RÚZ — Register of Financial Statements (registeruz.sk) — RECOMMENDED
- Base `https://www.registeruz.sk/cruz-public/api/`. **CC0**.
- `uctovne-jednotky?zmenene-od=YYYY-MM-DD[&ico=&dic=&pravna-forma=&max-zaznamov=≤10000&pokracovat-za-id=]`
  → `{id:[...], existujeDalsieId}` (paginated list of accounting-unit ids).
- `uctovna-jednotka?id={id}` → master record: `ico, dic, nazovUJ, mesto, ulica,
  psc, datumZalozenia, datumZrusenia, pravnaForma, skNace, velkostOrganizacie,
  druhVlastnictva, kraj, okres, sidlo, konsolidovana, idUctovnychZavierok[],
  idVyrocnychSprav[], zdrojDat, datumPoslednejUpravy`. Verified for ESET (id
  154048, 26 statements).
- `uctovna-zavierka?id={id}` → statement metadata: `obdobieOd/Do`,
  `datumZostaveniaK`, `typ` (Riadna/…), `idUctovnychVykazov[]`, dates.
- `uctovny-vykaz?id={id}` → the report: `obsah.tabulky[]` (each `{nazov.sk,
  data[]}`), `prilohy[]` (PDF attachments), `idSablony`. The `data[]` is a
  **positional** value array decoded against the template.
- `sablona?id={id}` → template (e.g. 687 "Úč MUJ"): `tabulky[]` →
  `riadky[]{cisloRiadku, oznacenie, text.sk}` — the line-item labels. Tables:
  Strana aktív (assets), Strana pasív (liabilities/equity), Výkaz ziskov a strát
  (income statement).
- Classifiers: `pravne-formy`, `sk-nace`, `kraje`, `okresy`, `sidla`,
  `druhy-vlastnictva`, `velkosti-organizacie`.
- **Note**: large/consolidated filers (e.g. ESET) may have empty `obsah` and only
  a PDF attachment ("Vybrané údaje"); most micro/small entities file the
  structured tables (verified populated for several 2026 reports, template 687).

### 3. ORSR — Obchodný register (orsr.sk) — secondary
- The underlying commercial register web portal. RPO already exposes its data via
  API, so ORSR scraping is unnecessary; not used.

### 4. FinStat / other aggregators — secondary
- Commercial aggregators mirror RPO + RÚZ with enrichment; restricted/paid bulk.
  Cross-check only; the official APIs supersede them.

## What was NOT bypassed

- No auth/payment/CAPTCHA involved; both APIs are open. Rate limits respected
  (sequential requests). Personal data in RPO (officers/shareholders/deposits)
  cataloged but to be **redacted** in published outputs.

## Recommended ingestion

Incrementally crawl RÚZ via `zmenene-od` (units → statements → reports, decoding
tables with cached templates) for master data + financials, and enrich
identity/officers/ownership/share-capital from RPO by IČO. Join on **IČO**.
