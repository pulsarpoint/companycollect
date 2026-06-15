# Japan Company Data Investigation

## Conclusion

Japan has an **excellent, fully-open identity register** plus **key-gated (but
free) financial sources**:

- **Identity (open bulk):** NTA Corporate Number Publication Site
  (法人番号公表サイト). Every registered corporation gets a unique **13-digit
  corporate number (法人番号)**. The full register is downloadable as national +
  per-prefecture bulk files (CSV Shift-JIS, CSV Unicode, XML), refreshed monthly
  with daily diffs, and a Web-API exists. The published basic information is
  designated freely usable.
- **Financials (free key):** EDINET (FSA) provides XBRL financial statements for
  listed and disclosure-obligated companies via API v2 (free Subscription-Key).
  gBizINFO (METI) aggregates financial info plus procurement, subsidies,
  certifications, and patents via a REST API (free token).
- **Full commercial register (paid):** officers, capital, purpose, and history
  live at the Legal Affairs Bureau registry (登記情報提供サービス), pay-per-record.

## What was verified live

- **NTA bulk download works.** Fetched the Tottori prefecture Unicode CSV via the
  public download form (file no. 27306): `31_tottori_all_20260529.zip` →
  `31_tottori_all_20260529.csv`, **20,153 corporations, 30 columns**. The download
  is a normal public form POST (CSRF token + session cookie) — no authentication,
  payment, or CAPTCHA, and no access control was bypassed.
- **EDINET v2 documents.json** returns HTTP **401** "invalid subscription key"
  without a key; v1 returns **403** (retired). So EDINET financials require a free
  Subscription-Key registration.
- **gBizINFO** `/hojin/v1/hojin` returns HTTP **500** without a token; the API doc
  page confirms 利用申請 → APIトークン and lists 財務情報 (financial info) among the
  provided datasets.

## Identifiers

- **法人番号 (Corporate Number)** — 13 digits (a 12-digit base number + 1 check
  digit). Assigned by the NTA to every registered corporation and to government
  bodies. It is **both the company id and the corporate taxpayer number**. There
  is **no separate VAT number** in Japan (the Qualified Invoice / インボイス
  registration number is `T` + the 13-digit corporate number).
- The corporate number is the universal **join key** across NTA, EDINET (exposed
  as JCN in filer metadata), and gBizINFO.

## NTA record structure (30 columns, verified)

corporate number, process/correction flags, update/change dates, name, name image
id, corporate kind (101 national agency / 201 local public body / 301 registered
corporation / 401 foreign company etc. / 499 other), domestic address
(prefecture / city / street + image id), prefecture & city codes, postal code,
overseas address (+ image id), registry closure date & cause, successor corporate
number, change cause, assignment date, latest-history flag, English name, English
prefecture/city, English overseas address, furigana (kana reading), and
search-exclusion flag.

This is **identity + address + status only**. No capital, no financials, no
officers, no industry classification.

## What is NOT openly available

- Financials of **non-listed** companies (only listed/disclosure-obligated via
  EDINET).
- Officers / directors / representatives, registered capital, company purpose,
  and full history — only via the paid Legal Affairs Bureau registry.
- An industry/activity code in the NTA open data (none present).

## Recommended ingestion

1. **NTA bulk** (national or per-prefecture) for the complete identity layer —
   fully open, monthly full + daily diff.
2. **EDINET API v2** (free key) for listed-company XBRL financials, joined on the
   corporate number.
3. **gBizINFO API** (free token) for enrichment (capital, employees, business
   summary, financial info, procurement, subsidies, certifications).
