# Mexico — License Notes

## INEGI DENUE — Términos de Libre Uso de la Información del INEGI

- DENUE is published under INEGI's **"Términos de Libre Uso de la Información del
  INEGI"** (free-use terms), which permit **free reproduction, redistribution, and
  reuse including commercial**, provided INEGI is **cited as the source** and the
  information is not presented in a way that distorts it.
- The **per-state CSV bulk** (`denue_{EE}_csv.zip`) needs **no token**; the
  query/consulta API needs a **free INEGI token**.
- Treatment here: **open / reusable with attribution**. Establishment + legal-name
  + activity data is business data, not personal data — but `telefono` /
  `correoelec` (contact details) may be personal data and are **redacted** in the
  committed sample.

## SAT 69-B list — public by law

- The 69-B list (taxpayers with presumed non-existent operations, art. 69-B of the
  CFF) is **public by statute** and explicitly published for consultation. Open CSV.
- Treatment here: **public / reusable**. The list contains **RFCs and legal names**
  of (mostly) companies in a **risk** context — handle responsibly; individual
  RFCs/names would be personal data (LFPDPPP) and should be treated with care.

## RPC / PSM commercial registry — per-document / fee-based

- The Registro Público de Comercio (RPC/SIGER) and PSM publications are official,
  but there is **no open bulk/API**; certified registry extracts are **fee-based**.
- Treatment here: **blocked_by_payment**. Cataloged from public documentation only;
  no values copied.

## BMV / CNBV — exchange terms

- Listed-company financial statements (EMISNET/SITI) are public but governed by
  **exchange/regulator terms of use**; verify before redistribution.
- Treatment here: **useful_secondary_source**; not fetched.

## datos.gob.mx — Libre Uso MX

- The national portal uses the **"Libre Uso MX"** licence (free reuse with
  attribution). The portal does not openly host the legal company register.

## Personal data

- **RFC and contact details of natural persons** are personal data under Mexico's
  **LFPDPPP**. Redact in committed/shared samples. Company RFCs and corporate names
  are corporate identifiers. The committed DENUE sample redacts phone/email.

## Tax identifiers

- The **RFC** is the tax id (12-char for companies). Mexico has **IVA (VAT)** but
  **no separate VAT number** — the RFC serves as the tax identifier.
