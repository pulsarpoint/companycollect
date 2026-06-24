# Montenegro — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data**
for companies registered in Montenegro, download/sample where allowed, and
document a reproducible trail.

## What was found

### 1. CRPS — Central Registry of Business Entities (official register; portal DOWN)

- **CRPS — Centralni registar privrednih subjekata** is Montenegro's company
  register, now operated by the **Uprava prihoda i carina** (Revenue and Customs
  Administration, formerly Poreska uprava), under `tax.gov.me` (contact
  `crps@tax.gov.me`, confirmed on the gov.me Tax Administration page).
- **Access problems (verified live):**
  - The **legacy portal `www.pretraga.crps.me` / `crps.me`** now serves a
    **domain-parking page** (`mydomaincontact.com/?domain_name=crps.me`) — the old
    CRPS domain has lapsed.
  - The current portal **`eprijava.tax.gov.me/TaxisPortal`** returned **HTTP 503
    "Service Unavailable"** on every attempt (root and `/TaxisPortal`,
    browser-like UA included) — the IIS application is down.
- So there is **no working open access point** for CRPS at investigation time, and
  **no open bulk or API** is published. CRPS is the authoritative source for
  identity (name, **PIB**, registration number, legal form, status, founders) and
  is where companies file **annual financial statements**.

### 2. data.gov.me — national open-data portal (working; no company register)

- **`data.gov.me`** is a **working CKAN** portal. Its API
  (`/api/3/action/package_search`) responds. But searching `privredna`,
  `preduzeca`, `kompanije`, `registar`, `biznis` returns **no company register** —
  only:
  - statistical aggregates from **Uprava za statistiku / MONSTAT** (construction
    activity, average wages, agriculture census),
  - niche registers (seed producers/importers, phytosanitary register),
  - a **"Javna preduzeća"** dataset (Ministry of Public Administration) — a real,
    openly-licensed **XLSX** list of **public/state enterprises** with name,
    status, type, founder, address, website. Verified entities include
    **Investiciono-razvojni fond Crne Gore A.D.**, **Crnogorski elektroprenosni
    sistem AD**, **Luka Bar AD**, **Pošta Crne Gore AD**, **Plantaže AD**, **Rudnik
    uglja AD Pljevlja**, **Crnogorska plovidba AD Kotor**, **MONTECARGO AD**,
    **Željeznički prevoz AD Podgorica**. This is **not** the full register.

### 3. MONSTAT — statistical office (aggregate)

- `monstat.org` is up; holds the **statistical business register**, but published
  output is **aggregate** (by sector/size), not company-level.

### 4. Financial data — not open

- Annual financial statements are filed with **CRPS / the tax administration** but
  are **not published openly**. `data.gov.me` has only statistical aggregates. So
  per-company financials are **not available openly**.

## Conclusion

Montenegro's official register (**CRPS**) is the right source but is currently
**inaccessible openly**: the legacy domain is **parked** and the current
**TaxisPortal is 503/down**, with **no open bulk/API**. The working open-data
portal **`data.gov.me`** does **not** host the company register — only statistics
and a **public-enterprises** list (real, openly licensed, used for the sample).
There are **no open financials**. The realistic path is **per-company CRPS lookup
once the portal returns** (keyed on PIB / name / registration number). Identifiers:
**PIB** (8-digit tax id), registration number, **PDV** (VAT). Currency **EUR**.
Founders/owners are personal data and must be redacted. No access controls were
bypassed.
