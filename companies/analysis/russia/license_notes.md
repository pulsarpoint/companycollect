# Russia — License Notes

## GIR BO (bo.nalog.gov.ru) — open

- The State Information Resource for Accounting (Financial) Statements is **open and
  free** to access (search API + financial-statements API + annual bulk datasets),
  with no key. The disclosed financial statements are public by law.
- Treatment here: **open**. Company identity (INN, OGRN, KPP, OKOPF, OKVED, region)
  and financial statements are corporate data, not personal data.

## RSMP + FNS open data (file.nalog.ru / nalog.gov.ru/opendata) — open data terms

- The Unified Register of SMEs (RSMP) and the FNS open datasets are published under
  the FNS / Russian **open-data** terms, generally permitting free reuse. Each
  dataset has a passport (паспорт) with the structure (XSD), update frequency, and
  publication dates. The RSMP archive is large (~2.25 GB monthly).
- Treatment here: **open / reusable**. Verify the per-dataset passport for any
  specific reuse condition.

## EGRUL (egrul.nalog.ru) — free per-company, paid full bulk

- The free per-company extract (выписка) is publicly accessible. The **full bulk**
  (all legal entities, daily) is provided via a **paid FTP subscription** under FNS
  terms.
- Treatment here: **blocked_by_payment** (full bulk). Directors/founders in EGRUL
  are **personal data** (handle per Russian data-protection law / 152-ФЗ) — redact.

## Access / compliance note

- The FNS systems were reachable from this environment at the time of investigation,
  but accessibility may vary by region/network. This is **public open data**; any
  user must ensure their own compliance with applicable laws and sanctions regimes
  when accessing Russian government systems. No access controls were bypassed.

## Personal data

- **Directors / founders / individual entrepreneurs** (EGRUL, RSMP ИП records) are
  **personal data** under Russia's **152-ФЗ** (and GDPR for EU users). Redact in
  committed/shared samples. The committed sample contains only legal-entity
  corporate identity + financial-statement years (Gazprom, Lukoil).

## Tax identifiers

- The **INN** is the tax id. Russia has **VAT (НДС)** but **no separate VAT
  number** — the INN is used. **KPP** is the tax-registration reason code.
