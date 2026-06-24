# Russia — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| ГИР БО (GIR BO) | FNS | financial statements | public, no key | JSON, ZIP, XML | open (FNS) | **recommended** |
| РСМП (RSMP SME register) | FNS | official registry | public bulk | XML, ZIP | open (FNS) | **recommended** |
| ЕГРЮЛ (EGRUL) | FNS | official registry | free per-company; paid full bulk | PDF, XML | free/paid | blocked_by_payment |
| FNS open data (tax info) | FNS | tax registry | public bulk | XML, CSV, ZIP | open (FNS) | useful_secondary_source |

## Roles

- **gir_bo** — open **financial statements + identity** (INN, OGRN, KPP, OKOPF,
  OKFS, OKVED, region, status), free API + bulk. Verified live (Gazprom, Lukoil).
  Covers non-bank/non-budget filers.
- **rsmp_sme_register** — open **company list** (SMEs): INN, OGRN, name, OKVED,
  category, headcount. Monthly bulk XML (~2.25 GB), XSD provided.
- **egrul** — authoritative full register (directors/founders/capital/history);
  free per-company extract, paid full bulk.
- **fns_opendata_taxinfo** — per-INN enrichment (headcount, tax regimes, taxes,
  arrears, disqualified persons).

## Join keys

**INN** (10-digit) and **OGRN** (13-digit) across all sources. Russia uses the INN
as the tax id (VAT/НДС has no separate number). KPP is the registration-reason code.
