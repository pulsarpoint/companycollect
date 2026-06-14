# Poland — License & Terms Notes

> Public ≠ unrestricted, but Poland's company sources are broadly open. Record attribution per source.

## KRS API (Ministerstwo Sprawiedliwości)
- The open KRS API is provided **free of charge** for public reuse; responses correspond to the official
  current/full extracts with **personal data anonymized** (GDPR). Reuse of public-register data is
  generally permitted; attribute the source (Krajowy Rejestr Sądowy / Ministerstwo Sprawiedliwości).
- No formal rate limit published — apply polite throttling.

## RDF — Financial statements
- The Repozytorium Dokumentów Finansowych offers **free access and download** of filed statements
  (XML/XBRL/PDF). The documents are public filings; reuse of the structured data is open. Mass automated
  access via the PRS-eKRS module requires registration. Attribute KRS / Ministerstwo Sprawiedliwości.

## Biała lista podatników VAT (Ministerstwo Finansów / KAS)
- Free public register via API + daily flat file. Reusable for contractor verification and beyond;
  respect the documented request limits. Attribute Ministerstwo Finansów / KAS. Contains **bank account
  numbers** and may include representatives (PII) — handle under GDPR.

## CEIDG
- Open public register of sole proprietors; API needs a **free token**. Records include the
  **entrepreneur's name and address (personal data)** — GDPR lawful basis + retention required.

## REGON / GUS BIR1
- GUS statistical register; **free API key**. Reusable with attribution (Główny Urząd Statystyczny).

## CRBR (beneficial ownership)
- Free public register. Beneficial owners are **natural persons (personal data)** — GDPR applies; reuse
  for legitimate purposes with care.

## dane.gov.pl
- License is **per dataset** (often open). Check each dataset's license field.

## Commercial aggregators
- Proprietary/freemium; redistribution per contract. They mostly resell the open KRS/RDF data.

## Personal data / GDPR
- KRS API anonymizes personal data, but CEIDG (entrepreneur names), CRBR (beneficial owners), and the
  white list (representatives) carry personal data. Apply a GDPR lawful basis + retention policy before
  persisting names beyond the raw zone.

## Summary recommendation
- **Free to ingest now (with attribution)**: KRS API, RDF financials, VAT white list, CEIDG, REGON, CRBR.
- Honor anonymization/PII boundaries; record source + retrieved_at on every record for attribution.
