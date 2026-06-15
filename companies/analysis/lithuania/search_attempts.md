# Lithuania — Search Attempts

## Attempt 1

- Date/time: 2026-06-16
- Source: Registrų centras + data.gov.lt
- URL: https://www.registrucentras.lt/ ; https://data.gov.lt/ ; https://get.data.gov.lt/
- Language: Lithuanian/English
- Why: Registrų centras runs the Register of Legal Entities (JAR); data.gov.lt is the national open-data portal.
- Result: all HTTP 200; get.data.gov.lt returns JSON (Spinta API).
- Decision: Pursue the data.gov.lt Spinta API for JAR.

## Attempt 2

- Date/time: 2026-06-16
- Source: get.data.gov.lt namespace browse
- URL: https://get.data.gov.lt/datasets/gov/rc/jar/:ns
- Language: Lithuanian
- Why: Enumerate JAR data models.
- Result: rich namespace — iregistruoti (JuridinisAsmuo), balanso_ataskaitos, pelno_ataskaitos, buveines, ja_kapitalas, valdymo_organai, formos_statusai, NGO/late-filer models.
- Decision: RECOMMENDED — open identity + financials, no key.

## Attempt 3

- Date/time: 2026-06-16
- Source: JuridinisAsmuo + Buveine models
- URL: .../jar/iregistruoti/JuridinisAsmuo?limit(3) ; .../jar/buveines/Buveine?limit(1)
- Language: Lithuanian
- Why: Capture identity schema + real records.
- Result: real records (ja_kodas, ja_pavadinimas, reg_data, isreg_data, forma._id, statusas._id, stat_data); addresses in Buveine.
- Decision: Used as the real normalized sample.

## Attempt 4

- Date/time: 2026-06-16
- Source: BalansoAtaskaita + PelnoAtaskaita
- URL: .../jar/balanso_ataskaitos/BalansoAtaskaita?limit(1) ; .../pelno_ataskaitos/PelnoAtaskaita?limit(1)
- Language: Lithuanian
- Why: Confirm open financial statements.
- Result: real line items — TRUMPALAIKIS TURTAS €13,532 (2023); PARDAVIMO PAJAMOS €58,708 (2021). EUR, per period, linked to a company.
- Decision: RECOMMENDED — open financials.

## Attempt 5

- Date/time: 2026-06-16
- Source: Forma + Statusas code lists
- URL: .../jar/formos_statusai/Forma ; .../formos_statusai/Statusas
- Language: Lithuanian/English
- Why: Resolve legal-form and status references (kodas, LT + EN labels).
- Result: 168 forms, 31 statuses with both Lithuanian and English names.
- Decision: Use to resolve forma/statusas `_id` references. (Rapid bursts occasionally returned curl 000 — paced requests.)
