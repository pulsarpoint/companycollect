# Hungary — Company Open Data Investigation

## Conclusion

Hungary is a **partial-open** country. Financial statements are **free to view** and basic company identity is
**free to search**, but there is **no open bulk export**, the financial-statements search is **reCAPTCHA-
protected**, and full register data (officers, owners, history) is **paid**. Everything joins on the
**cégjegyzékszám** (company registration number, format `NN-NN-NNNNNN`) and the **adószám** (tax number).

## What was verified (live)

- **e-beszámoló** `e-beszamolo.im.gov.hu/` → HTTP 200. The financial-statements portal of the Ministry of
  Justice. All annual reports (**beszámoló**) are free to view by company name / cégjegyzékszám / adószám, no
  registration. Homepage search form posts to **`/Search/Results`** (`firmName`, `firmNumber`, `firmTaxNumber`).
  - A live `POST /Search/Results firmName=MOL` returned **`{"errorText":"A reCaptcha kitöltése nem
    megfelelő."}`** ("the reCaptcha completion is invalid"), and the page loads **recaptcha.js** → the search is
    **reCAPTCHA-protected**. Automated/bulk access requires a reCAPTCHA token → **not bypassed**.
- **e-cégjegyzék** `www.e-cegjegyzek.hu/` → HTTP 200, title **"Cégszolgálat Ingyenes Céginformáció"** (free
  company information). Free basic search; certified/full extracts (cégkivonat) are paid.
- **NAV** `nav.gov.hu/` → HTTP 200. VAT-subjects (**áfaalanyok**) databases reachable
  (`/adatbazisok/adatbleker/afaalanyok/afaalanyok_egyszeru` and `…_csoportos`, HTTP 200, ~257 KB CMS pages),
  updated **daily**. Single + group/batch query (upload adószám list → VAT status); some lists downloadable CSV
  (e.g. excise subjects). Pages contain "Letöltés" (download) links.
- **VIES** validates HU EU VAT (közösségi adószám = `HU` + 8-digit base).

## Identifiers

- **Cégjegyzékszám** — company registration number, format `NN-NN-NNNNNN` (court code – form code – serial);
  the register-side join key.
- **Adószám** — 11-digit tax number: `XXXXXXXX-Y-ZZ` = 8-digit **törzsszám** (base) + 1 VAT code + 2 county
  code. The 8-digit base is the universal stem.
- **Közösségi adószám** (EU VAT) = `HU` + the 8-digit base.
- **Statisztikai számjel** — 17-digit statistical code (embeds the 8-digit base + TEÁOR + legal form + county),
  maintained by KSH.
- **TEÁOR** — Hungarian activity classification (NACE-aligned).

## Financial data

- Companies must e-file annual financial statements (**beszámoló**: **mérleg** balance sheet +
  **eredménykimutatás** income statement) to e-beszámoló. They are **public and free** and expose **structured
  key figures** (sales revenue, profit after tax, assets, equity, liabilities) plus the PDF and an electronic
  form (XML). Currency **HUF** (some report in EUR). Failure to file leads to tax-number cancellation / forced
  dissolution — so coverage is high.
- **But** the portal search is **reCAPTCHA-gated** → no lawful open automation. Structured financials at scale
  therefore need a **commercial provider** (which parse e-beszámoló) or manual lookups.

## Recommended ingestion

No lawful open bulk/automation path. Options: (a) **manual** e-beszámoló / e-cégjegyzék lookups; (b) a
**commercial provider** (OPTEN, Bisnode, Céginfo, companyapi.hu) for full register + structured financials at
scale; (c) **NAV áfaalany** batch query + **VIES** to validate/enrich tax numbers. KSH for the statistical code
and TEÁOR.

## Risks / open questions

- **Access controls**: e-beszámoló search is reCAPTCHA-protected — must not be bypassed.
- **Paid full register**: officers/owners/history require payment via the Céginformációs Szolgálat or a vendor.
- **License**: reuse/redistribution terms for register/financial data are not clearly stated — confirm first.
- **No open mirror / bulk**: no sanctioned open bulk of the cégjegyzék or financials.
- **GDPR**: officers/representatives are personal data.
