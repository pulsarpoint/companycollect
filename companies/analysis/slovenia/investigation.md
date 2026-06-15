# Slovenia Company Data — Investigation

## Conclusion

Slovenia has **fully-open identity + tax data** but **no open structured
financials**. Two free official open datasets (both CC-BY 4.0) combine on the
**matična številka** (registration number):

- **AJPES PRS — Poslovni register Slovenije** (via OPSI): the business register —
  matična številka, full name, legal form, registrar, full address. 293,222
  entities. CSV (UTF-16), twice-monthly.
- **FURS — Seznam davčnih zavezancev / legal entities** (Financial
  Administration, via OPSI): davčna številka (tax number), VAT liability, SKD
  activity code, name, address, tax office. 144,537 legal entities. CSV (ZIP),
  daily.

Financials are **public but not open**: AJPES **JOLP** lets anyone view a
company's annual reports (balance sheet, income statement, ~5 years) for free, but
there is **no open bulk/API**; the structured financial database (**Fi=Po**) and
**S.BON** ratings are **paid**.

## Identifiers

- **matična številka** — registration number (10-digit for the open feeds; the
  base 7-digit unit + suffix). Universal join key (PRS ↔ FURS).
- **davčna številka** — tax number (8-digit). From FURS. **VAT id = `SI` +
  davčna** when VAT-registered.
- **HSEID/HSMID** — unique address/building identifier (in PRS).
- **SKD** — Standardna klasifikacija dejavnosti (≈ NACE Rev.2); activity code in
  FURS (e.g. 49.410).

## Sources found

### 1. AJPES PRS — business register (OPSI CSV) — RECOMMENDED
- Download `https://podatki.gov.si/dataset/9ee1a9aa-c224-4995-b2ad-3760d7af0748/resource/beb70929-3d0d-41c6-9af2-25d525d906d3/download/opsiprs.csv`
  (127 MB, **UTF-16**, comma-delimited, quoted).
- Columns: `Matična številka, Popolno ime, HSEID, Pravnoorganizacijska oblika,
  Registrski organ, Ulica, Hišna št, Hišna št dodatek, Naselje, Poštna št, Pošta,
  Država`. 293,222 rows. **CC-BY 4.0**, refreshed twice monthly.
- Companion `ATRIBUTI_MR_20.xml` (codelist/attribute definitions, 36 KB).
- Note: covers all entity types (d.o.o., s.p., društvo, poslovna enota, javni
  zavod, …). No tax number, no status, no SKD, no financials in this open feed.

### 2. FURS — Seznam davčnih zavezancev (legal entities) — RECOMMENDED
- Download `https://www.fu.gov.si/fileadmin/prenosi/DURS_zavezanci_PO_csv.zip`
  (8 MB; `DURS_zavezanci_PO.csv`, **UTF-8 BOM**, **semicolon**-delimited).
- Columns: `Omejen obseg identifikacije; Zavezanost za DDV; Davčna številka;
  Matična številka; Datum registracije za DDV; Šifra dejavnosti; Ime zavezanca;
  Naslov zavezanca; Finančni urad`. 144,537 legal entities. **CC-BY 4.0**, daily.
- `Zavezanost za DDV` = `*` marks a VAT payer (→ VAT id `SI` + davčna).
- Companion datasets: natural persons performing activity (DEJ), VAT natural
  persons (FO), VAT-ID revocation lists.

### 3. AJPES restPrsInfo — REST web service — credentialed
- REST service (JSON/XML), searchable by matična/davčna/name/address/activity;
  minimal / ožja / širša data tiers; plus a change-list endpoint for incremental
  updates. **Requires registration/credentials** (AJPES FTP username/password)
  and is **explicitly not for mass download**. Doc:
  `https://www.ajpes.si/Doc/AJPES/Za_razvijalce/restPrsInfo_Opis_servisa_za_razvijalce.pdf`.
  → blocked_by_authentication for bulk; useful for the broader fields (status,
  SKD, history) per entity once access is granted.

### 4. AJPES JOLP — annual reports (financials) — view-only
- `https://www.ajpes.si/jolp/` — free public view of filed annual reports
  (balance sheet, income statement) for ~the last 5 years. Per-company web
  pages/PDF; **no documented open bulk or API**. → financials are viewable but not
  openly downloadable.

### 5. AJPES Fi=Po / S.BON — paid financial products
- **Fi=Po**: database of complete financial statements + indicators (paid login).
- **S.BON**: AJPES credit-rating scores (paid). → blocked_by_payment.

## What was NOT bypassed

- `restPrsInfo` credentials, JOLP per-company access, and the paid Fi=Po/S.BON
  products were not circumvented. Only the OPSI/FURS open CSVs were downloaded.

## Recommended ingestion

Bulk-load PRS + FURS CSVs and join on **matična številka** for identity +
tax/VAT/SKD activity. Mind encodings (PRS UTF-16; FURS UTF-8 semicolon). For
financials, JOLP is view-only and Fi=Po is paid — no open structured feed.
