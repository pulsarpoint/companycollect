# North Macedonia — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data**
for companies registered in North Macedonia, download/sample where allowed, and
document a reproducible trail.

## Environment limitation (important)

The official North Macedonia hosts were **unreachable from this environment**:

- `crm.com.mk` / `www.crm.com.mk` — **DNS resolves** (`crm.com.mk` →
  `92.55.95.145`) but **every HTTPS/HTTP request timed out** (curl exit 28 / HTTP
  000), including the `e-submit.crm.com.mk` distribution host.
- `data.gov.mk` — CKAN API also timed out (HTTP 000).
- `ujp.gov.mk` (Public Revenue Office) — returned **HTTP 502**.

DNS resolving while TCP/HTTP fails indicates a **network-level block/firewall**
between this environment and the .mk government IPs (the same pattern seen earlier
for some other government hosts), **not** that the sites are down. Consequently the
content below is documented from **established public documentation** of the Central
Registry; **no live values were captured** and **no identifiers were fabricated**.

## What the sources are (from public documentation)

### 1. Central Registry of North Macedonia — CRM (official register)

- **Централен регистар на Република Северна Македонија** (`crm.com.mk`) operates:
  - the **Trade Registry** — *Трговски регистар и регистар на други правни лица*
    (companies and other legal entities), and
  - the **Registry of Annual Accounts** — *Регистар на годишни сметки* (annual
    financial statements).
- **Identifiers**: **ЕМБС** (Единствен матичен број на субјектот) — 7-digit unique
  entity registration number = company id; **ЕДБ** (Единствен даночен број) —
  13-digit tax number; **ДДВ** (VAT registration).
- **Access model**: the CRM offers a **free public search** ("Пребарување") for
  basic fields (existence, name, ЕМБС, status), and is the official **commercial
  distributor of data** — **bulk extracts, detailed company data, and financial
  statements are paid** (subscription / per-document via the e-distribution /
  дистрибуција на податоци service). So: free per-company basic search;
  **blocked_by_payment** for bulk and financials.
- **Record fields** (Trade Registry): назив/име (name), ЕМБС, ЕДБ, правна форма
  (legal form), седиште/адреса (registered seat/address), дејност (activity, NKD
  ~NACE), статус (status), управители/основачи (managers/founders), основна
  главнина (capital).

### 2. Registry of Annual Accounts — financial statements (paid)

- All companies file **annual accounts** (годишна сметка): **Биланс на состојба**
  (balance sheet) and **Биланс на успех** (income statement), in **MKD**. Held by
  the CRM and available through its **paid distribution**; no open bulk.

### 3. UJP — Public Revenue Office (tax/VAT)

- **Управа за јавни приходи** (`ujp.gov.mk`) administers the **ЕДБ** and **ДДВ
  (VAT)**; per-company tax registration. (Returned 502 at investigation time.)

### 4. data.gov.mk — open data portal

- National **CKAN** open-data portal exists, but it does **not** host the full
  company register (typically statistics / sectoral datasets). Unreachable from
  this environment to confirm specific datasets.

## Conclusion

North Macedonia's official register is the **Central Registry (CRM)**, which holds
both company identity (keyed on **ЕМБС**, with **ЕДБ** as tax id) and **annual
financial statements** — but the **register and financials are commercially
distributed (paid)**, with only a **free basic per-company search** open. There is
**no open bulk register and no open financials**. This investigation was further
constrained because the .mk government hosts were **firewalled from this
environment** (DNS resolves; TCP/HTTP blocked), so the model is documented from
public sources and the sample carries **public-knowledge legal names with null
identifiers** (nothing fabricated). Founders/managers are personal data and must be
redacted. Currency **MKD**.
