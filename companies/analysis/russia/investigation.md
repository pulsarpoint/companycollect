# Russia Company Data Investigation

## Conclusion

Russia has **strong open financial data** (GIR BO) and an **open SME register**
(RSMP), while the **full authoritative company register (EGRUL) bulk is paid**:

- **Financials + identity (open API):** **ГИР БО (GIR BO)** at `bo.nalog.gov.ru` —
  the State Information Resource for Accounting (Financial) Statements (FNS). Free
  search API returns identity (INN, OGRN, KPP, OKOPF legal form, OKFS ownership,
  OKPO, OKVED, region/address, status) and the list of filed **annual financial
  statements** per year, plus annual bulk datasets. RUB.
- **Company list (open bulk):** **РСМП (RSMP)**, the Unified Register of SMEs —
  open monthly **bulk XML** at `file.nalog.ru` (latest ~2.25 GB, XSD provided): INN,
  OGRN, names, region, OKVED, SME category, headcount. ~6M SMEs.
- **Authoritative register (paid bulk):** **ЕГРЮЛ (EGRUL)** — free per-company
  extract (выписка); the full bulk is via a **paid FTP subscription**.
- **Enrichment (open):** FNS publishes formerly-tax-secret info per INN (headcount,
  tax regimes, income/expense, paid taxes, arrears) and the disqualified-persons
  register.

## What was verified live

- **GIR BO search API works** (no key): real companies — **ПАО "ГАЗПРОМ"** (INN
  7736050003, OGRN 1027700070518; financial statements for **2021–2025**), **ПАО
  "ЛУКОЙЛ"** (INN 7708004767, OGRN 1027700035769). The org record carries INN, OGRN,
  KPP, OKOPF (e.g. "Публичные акционерные общества"), OKFS, OKPO, OKVED, region,
  address, status. Banks (Sberbank 7707083893) are **excluded** (they file with the
  Central Bank).
- **RSMP open dataset**: bulk ZIP at file.nalog.ru (latest `data-10062026…`,
  `content-length` 2,247,152,251 ≈ **2.25 GB**) + an **XSD** structure (downloaded).
- **FNS opendata listing** loads with many per-INN datasets (rsmp, revexp, paidtax,
  sshr, registerdisqualified, …). **EGRUL** redirects to a JS app (free per-company
  выписка); **data.gov.ru** is unreachable (TLS error).

## Identifiers

- **ОГРН (OGRN)** — **13-digit** primary state registration number — the company id.
- **ИНН (INN)** — **10-digit** taxpayer id for legal entities (12 for individuals).
- **КПП (KPP)** — **9-digit** tax-registration reason code (varies by branch).
- Russia has **VAT (НДС)** but **no separate VAT number** — the INN is the tax id.
- **Classifiers**: OKVED (economic activity), OKOPF (legal form), OKFS (ownership
  form), OKPO, OKATO/OKTMO (territory).

## GIR BO org/financial schema (verified)

Search result: `id`, `inn`, `ogrn`, `shortName`, `index`, `region`, `district`,
`city`, `settlement`, `street`, `house`, `building`, `office`, `okved2`, `okopf`
(`{id,name}`), `okfs` (`{id,name}`), `okpo`, `okato`, `statusCode`, `statusDate`,
`bfo` (list). BFO record: `id`, `period`, `knd` (form code), `organizationInfo`
(`fullName`, `inn`, `kpp`, `okpo`, `okopf`, `okfs`), and the financial forms
(Бухгалтерский баланс / Отчёт о финансовых результатах) per period, RUB.

## RSMP XSD schema (verified)

`ОргВклМСП` (legal entity): `НаимОрг` (full name), `НаимОргСокр` (short name),
`ИННЮЛ` (INN), `ОГРН`; `ИПВклМСП` (individual entrepreneur): `ФИОИП`, `ИННФЛ`,
`ОГРНИП`; `СведМН` (location): `Регион`, `Район`, `Город`, `НаселПункт`,
`КодРегион`; `СвОКВЭД` (activity): `СвОКВЭДОсн` (main), `СвОКВЭДДоп` (additional),
`КодОКВЭД`; `ДатаВклМСП` (date included), `ВидСубМСП` (entity type ЮЛ/ИП),
`КатСубМСП` (category micro/small/medium), `ССЧР` (average headcount), licenses,
products, social-enterprise flag.

## What is NOT openly available (free)

- **The full EGRUL bulk** — paid FTP subscription (free per-company extract only).
- **Directors / founders** — in EGRUL (paid/per-company); personal data.
- **Bank financials** — not in GIR BO (Central Bank instead).
- A separate VAT number — Russia uses the INN.

## Recommended ingestion

1. **GIR BO** — search API per INN/OGRN for identity, then the financial-statements
   API per period (RUB); or the annual bulk datasets.
2. **RSMP** open XML — the SME company list (INN, OGRN, name, OKVED, category).
3. **FNS open data** per-INN sets for enrichment (headcount, tax regime, taxes).
4. Treat the **EGRUL full bulk** as a paid subscription; redact directors/founders
   (personal data) if obtained.
