# Albania Company Data Investigation

## Conclusion

Albania's commercial register is **openly mirrored** by Open Data Albania, with the
official register at the QKB:

- **Identity (open):** the **QKB (Qendra Kombëtare e Biznesit)** commercial register
  is republished as **open data** on **opencorporates.al** (Open Data Albania / AIS)
  — company lists + per-company pages keyed on **NIPT/NUIS**. Fields: name, NIPT,
  administrator, owners (ortakë), capital, activity (objekti), status, former names.
- **Official register:** **qkb.gov.al** provides the authoritative per-company
  extract (ekstrakt) by NIPT/name. No open bulk (per-company).
- **Financials:** annual statements (bilanci/pasqyrat financiare) are filed with
  QKB; Open Data Albania publishes some indicators. No clean open bulk.

## What was verified live

- QKB / opencorporates.al / opendata.gov.al / tatime.gov.al all reachable (200).
- **opencorporates.al** company list yielded **4,459 NIPTs** (letter+8digits+letter)
  with names — e.g. NEXUS GROUP (`L67508702G`), MALESIA TRAVEL (`L61307015S`),
  MEDIAL (`L61306025U`), SAFETY ALBANIA (`L61308059O`). Per-company pages at
  `/sq/company/{NIPT}`.
- opendata.gov.al CKAN API not at the standard path; opencorporates.al is the
  practical open source.

## Identifiers

- **NIPT/NUIS** (Numri i Identifikimit për Personin e Tatueshëm) — the unique
  business identifier: **letter + 8 digits + letter** (e.g. `K12345678L`). It is the
  **company id, the tax id, and the VAT id** — Albania has VAT and the NIPT serves
  as the VAT number.

## Register fields (QKB / Open Data Albania)

NIPT, emri (name), forma ligjore (legal form: Sh.p.k. = Ltd, Sh.a. = JSC, Person
Fizik = sole trader), data e regjistrimit (registration date), administrator,
ortakë/aksionarë (owners/shareholders), kapitali (capital), objekti i veprimtarisë
(activity), adresa (address), statusi (status: active/closed), emra të mëparshëm
(former names, "ish").

## What is NOT openly available

- A single clean open **bulk** dataset / API (per-company pages + lists).
- **Financial statements** in clean open bulk (filed with QKB).
- Administrator/owners are personal data — redact.

## Recommended ingestion

1. **Open Data Albania (opencorporates.al)** company lists + per-company pages keyed
   on NIPT.
2. **QKB extract** for authoritative detail.
3. Redact administrator/owner personal data.
