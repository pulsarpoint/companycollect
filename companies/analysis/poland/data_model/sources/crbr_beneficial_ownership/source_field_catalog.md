# CRBR — Beneficial Ownership — Field Catalog

> **OPEN, free public** beneficial-ownership register (unusual vs many EU peers post-CJEU). Lookup by NIP.
> Beneficial owners are **natural persons → sensitive PII** (incl. PESEL) — minimize/redact. Cataloged from
> public docs; no sample retrieved (PII).

## Source Summary

- Country: Poland
- Source type: beneficial_ownership_register
- Organization: Ministerstwo Finansów
- URL: https://crbr.podatki.gov.pl/ (lookup by NIP; adcrbr API)
- License: open (free public register)
- Access: public, no auth
- Freshness: continuous
- Record shape: per-entity (by NIP) with `beneficjenci[]`
- Primary keys: `nip + beneficjent`
- Join keys: `nip`

## Fields

| Path | Source field (PL) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| podmiot.nip | nip | Entity NIP | string | identifier | join |
| beneficjenci[].imię/nazwisko | imię/nazwisko | Owner name | string | person | **PII** |
| beneficjenci[].obywatelstwo | obywatelstwo | Citizenship | string | person | PII |
| beneficjenci[].udziały/charakterUprawnień | udziały/uprawnienia | Ownership/control | string | ownership | the BO signal |
| beneficjenci[].PESEL/dataUrodzenia | PESEL / data urodzenia | Owner id | string | person | **sensitive — minimize** |
| dataZgłoszenia | data zgłoszenia | Filing date | date | date | |

## Interpretation Notes

- **Open ownership** — Poland exposes beneficial owners for free, an advantage over DE/IT (restricted).
  Join by **NIP** to the company spine.
- **Sensitive PII**: names + citizenship + **PESEL/birth date**. Apply a strict GDPR lawful basis; do **not**
  persist PESEL beyond the raw zone (minimize to birth year if needed); consider redaction by default.
- The ownership/control field describes the **nature/percentage** of beneficial control (free text/coded).
- No `sample_record.json` (personal data; not retrieved).
