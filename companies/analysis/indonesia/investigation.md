# Indonesia — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Indonesia, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## What was found

### 1. AHU Online — legal-entity registry (official; geo-blocked + paid)

- **`ahu.go.id`** — Direktorat Jenderal Administrasi Hukum Umum (Ditjen AHU),
  **Ministry of Law** (Kemenkumham). The authoritative **legal-entity registry** for
  **PT** (Perseroan Terbatas), CV, Firma, **Yayasan**, Perkumpulan. Records the
  legal identity: nama PT, **nomor SK pengesahan badan hukum**, NPWP, modal dasar/
  disetor (capital), pengurus (directors/commissioners), pemegang saham
  (shareholders).
- A public **"Pencarian Profil Perseroan"** exists, but full profiles and legal
  documents are **paid (PNBP — Penerimaan Negara Bukan Pajak)**.
- **Access (verified):** `ahu.go.id` **resolves via DNS** (`103.200.129.129`) but
  **every HTTPS request timed out** (HTTP 000) from this environment — a
  **network-level block**, not a site outage. No open bulk/API. Documented from
  public knowledge; no values captured.

### 2. OSS — Online Single Submission (NIB issuer; works, per-company)

- **`oss.go.id`** — **Kementerian Investasi/BKPM**. Issues the **NIB** (Nomor Induk
  Berusaha), the modern business identification number, plus risk-based business
  licenses, tied to **KBLI** activity codes. The public site loads (Next.js SPA) and
  offers **"Cari NIB"** (search business actors) and **`/id/pencarian`**.
- The NIB search is **per-company** and JS-driven; specific data endpoints were not
  openly enumerable (SPA paths 404 on direct GET), and there is **no open bulk
  register**. OSS/BKPM publishes **aggregate investment statistics**, not a
  company-by-company dataset.

### 3. IDX — listed-company financials (open via browser; Cloudflare-gated)

- **`idx.co.id`** — Bursa Efek Indonesia. Publishes **listed-company financial
  statements** (laporan keuangan), annual reports, and company profiles — the main
  **open financial** source (~900 listed companies, e.g. **BBCA**, **TLKM**,
  **ASII**).
- **Access (verified):** the listed-company API returned **HTTP 403 with a
  Cloudflare "Attention Required" challenge** — public via the browser but
  **Cloudflare-gated** for automation. **Not bypassed.**

### 4. Satu Data Indonesia — national open-data portal (works; no register)

- **`data.go.id`** (Portal Satu Data Indonesia) loads, but it aggregates **regional
  and sectoral statistics** (health, agriculture, education by kabupaten) — **no
  national company register**. The CKAN-style API was not at the standard
  `/api/3/action` path (404).

### 5. Tax / private financials

- **DJP/Pajak** issues the **NPWP** (tax id); per-company. Private-company financial
  reports (**LKTP**, filed under the company-registration obligation with the
  Ministry of Trade) are **not openly public**.

## Conclusion

Indonesia's company identity is split across **AHU** (legal entity: PT/CV/Yayasan,
authoritative, **paid + geo-blocked** here) and **OSS** (the **NIB** business id,
per-company). **Listed-company financials** are openly published by **IDX** but
**Cloudflare-gated** for automation. The national open-data portal **does not** host
the register, and **private-company financials are not open**. The realistic path is
**per-company lookup** (AHU profile [paid] + OSS NIB) joined on company identity,
with **IDX** for listed financials. Identifiers: **NIB / NPWP / SK-AHU**; VAT is PPN
(no separate number; PKP status). Currency **IDR**. Directors/shareholders are
personal data and must be redacted. No access controls were bypassed; the sample
uses **public-knowledge listed companies with null identifiers** (nothing
fabricated).
