# Company data sources for Russia

## Status

- Official bulk data: **found** (GIR BO financial-statements datasets; RSMP SME-register XML — both open)
- Official API: **found** (GIR BO search + financial-statements API; no key)
- Open data portal: **found** (nalog.gov.ru/opendata + file.nalog.ru bulk; bo.nalog.gov.ru)
- License: open-data terms (FNS open data / GIR BO public)
- Recommended ingestion path: **API + bulk** (GIR BO for identity + financials; RSMP XML for the SME company list)

## Best source

**ГИР БО — GIR BO** (`bo.nalog.gov.ru`), the State Information Resource for
Accounting (Financial) Statements, run by the **Federal Tax Service (FNS)**. It is
**open and free**: a search API returns company **identity** (INN, OGRN, KPP, OKOPF
legal form, OKFS ownership, OKPO, OKVED activity, region/address, status) and the
**list of filed annual financial statements** (balance sheet + income statement)
per year, plus annual **bulk datasets**. Covers essentially all non-bank,
non-budget legal entities that file accounts.

Verified live: the search API returned **ПАО "ГАЗПРОМ"** (INN 7736050003, OGRN
1027700070518; financial statements for 2021–2025) and **ПАО "ЛУКОЙЛ"** (INN
7708004767, OGRN 1027700035769). Banks (e.g. Sberbank) are excluded from GIR BO
(they file with the Central Bank).

## The company register (EGRUL) + the SME register (RSMP)

- **ЕГРЮЛ (EGRUL)** — the authoritative Unified State Register of Legal Entities
  (FNS, `egrul.nalog.ru`). Free **per-company extract** (выписка, PDF); the **full
  bulk** is via a **paid FTP subscription** (no free open bulk).
- **РСМП (RSMP)** — the **Unified Register of SMEs** (`file.nalog.ru` open data) —
  **open bulk XML** (monthly; the latest archive is ~2.25 GB; an XSD structure is
  provided). Per entity: INN (ИННЮЛ), OGRN, full + short name, region, OKVED
  activity, SME category (micro/small/medium), date included, average headcount.
  The best **open** company list (covers ~6M SMEs — the bulk of Russian companies).

## Financial data — open, verified

**GIR BO** is the open financial-statements source: annual accounts (Бухгалтерский
баланс / Отчёт о финансовых результатах), keyed on INN/OGRN, in RUB, via API and
bulk datasets. Verified: Gazprom has statements for 2021–2025.

## Identifiers & tax

- **ОГРН (OGRN)** — 13-digit primary state registration number (company id).
- **ИНН (INN)** — 10-digit taxpayer id for legal entities (12 for individuals).
- **КПП (KPP)** — 9-digit tax-registration reason code.
- Russia has **VAT (НДС)** but **no separate VAT number** — the INN is the tax id.
- Classifiers: OKVED (activity), OKOPF (legal form), OKFS (ownership), OKPO,
  OKATO/OKTMO (territory).

## Caveats

- The FNS sites were reachable from this environment, but accessibility may vary by
  region/network. This is public open data; users must ensure their own compliance
  with applicable laws/sanctions when accessing Russian government systems.

## Next action

Ingest **GIR BO** (search API for identity + the financial-statements API per
INN/OGRN) and the **RSMP** open XML for the SME company list. EGRUL full bulk is a
paid FTP subscription. Sample uses real GIR BO data.
