# China — License Notes

## GSXT (National Enterprise Credit Information Publicity System)

- The portal provides **per-company public viewing**, but publishes **no open
  re-use licence** and **no open bulk/API**. Treat the data as **restricted for
  re-use** absent an agreement. Queries require **real-name authentication** (since
  Nov 2021) and a **CAPTCHA** — do **not** bypass these controls or run automated
  queries; the site is also frequently unreachable externally (HTTP 521).
- **Personal data**: the **legal representative** (法定代表人) and any
  shareholder/officer names are personal data under China's **PIPL** (Personal
  Information Protection Law) — redact/minimise; cross-border transfer of personal
  data is tightly regulated.

## cninfo / SSE / SZSE / BSE (listed-company financials)

- Listed-issuer disclosures are **public to view** but governed by the exchanges'/
  CSRC's disclosure terms — not an open-data licence. **Listed companies only.**
  Confirm redistribution terms before republishing documents.

## Credit China

- Credit/penalty portal; some datasets are downloadable but the **company register
  is not openly published** there. Bot-protected; reuse terms unclear.

## Commercial aggregators (Qichacha / Tianyancha / Aiqicha)

- **Paid / restricted vendor terms**; they resell GSXT-derived data. License
  uncertain — cross-check only; no raw values copied here.

## Data-sovereignty note

- China's data-export rules (PIPL, Data Security Law) restrict transferring
  personal information and certain data out of China. Any pipeline handling
  Chinese company data with personal information must account for these rules.

## Summary

- **No open data**: the register is gated/paid; financials are listed-only.
- **Personal-data caution**: legal-representative / shareholder names (PIPL).
- **Financials**: listed-only (cninfo/SSE/SZSE); non-listed not disclosed.
