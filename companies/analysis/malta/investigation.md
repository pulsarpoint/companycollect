# Malta — Company Open Data Investigation

## Conclusion

Malta is a **partial-open / automation-blocked** country. The authoritative register, the **MBR (Malta Business
Registry)**, is **publicly searchable for free** (English, no account), and the register holds rich data —
basic info, **officers**, **shareholders** (name, share type, degree of control), and **financial information**
(annual accounts, annual return). But there is **no open bulk export**, the online registry portals are
**WAF-blocked (HTTP 403)** for automated access, documents are **paid** (EUR 5–25), and the sanctioned automation
path is the **paid MBR API packages**. Everything joins on the **registration number** (e.g. `C 12345`).

## What was verified (live)

- **MBR** `mbr.mt` → HTTP 200; `mbr.mt/promo/company-search/` → HTTP 200 (confirms **Free** search + paid
  **document** purchase).
- **Registry portals** `registry.mbr.mt` and `baros.mbr.mt/app/home` → **HTTP 403** (WAF blocks non-browser
  clients) → automated/bulk access blocked.
- **data.gov.mt** → HTTP 403 to non-browser clients (WAF); standard CKAN/DKAN/uData API paths → 404 (non-standard
  custom portal). No open company register/financials bulk.
- **OpenCorporates** has the Malta register (registers/152). No OpenSanctions/open mirror found to lean on.
- WebSearch confirmed: free basic search; certified documents EUR 5–25; the MBR has **launched API packages**
  (Company Search API) for digital service delivery; UBO access restricted to **legitimate interest** since July
  2025 (post-CJEU).

## Identifiers

- **Registration number** — e.g. `C 12345`; the prefix encodes the entity class (**C** = companies / limited
  liability; partnerships and other forms use other prefixes). The register-side join key.
- **VAT number** — `MT` + 8 digits; separate from the registration number → VIES/CFR.
- **Income Tax Registration Number / TIN** — separate tax identifier (not in the free register data).

## Financial data

- Companies file **annual accounts** (under the Companies Act; **IFRS** or **GAPSME** for small companies) plus
  an **annual return** to the MBR. They are **public** but accessed as **paid documents** (EUR 5–25 per
  document), usually **PDF**. **Small companies file abridged accounts.** Currency **EUR**.
- There is **no open bulk export** of the figures. Structured financials at scale therefore need OCR/parsing of
  the paid PDFs, the **paid MBR API**, or a **commercial provider**.

## Recommended ingestion

No lawful open bulk/automation path. Options: (a) **manual** MBR lookups (basic info free; documents paid);
(b) the **paid MBR API packages** (the sanctioned automation route); (c) a **commercial provider** (Kyckr,
Creditinfo) for the register + structured financials at scale. Validate VAT via **VIES/CFR**.

## Risks / open questions

- **Access controls**: registry portals + data.gov.mt are WAF-blocked (403) — must not be bypassed; no open API.
- **Paid**: documents (EUR 5–25), the MBR API packages, and structured financials at scale.
- **License**: MBR reuse/redistribution terms not clearly stated — confirm before redistribution.
- **UBO** (beneficial ownership) restricted to legitimate interest (post-CJEU). Officer/shareholder data = GDPR.
- **No open bulk / mirror**: no sanctioned open bulk of the MBR; OpenCorporates indexes it.
