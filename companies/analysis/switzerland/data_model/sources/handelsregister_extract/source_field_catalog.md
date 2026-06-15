# Handelsregisterauszug / Cantonal Register Extracts Field Catalog

> **PLANNING-ONLY / PAID.** Certified extracts + documents sold per company by the
> 26 cantonal commercial registers. Cataloged from public docs; no records
> retrieved. PERSONAL DATA (officers).

## Source Summary

- Country: Switzerland
- Source type: official_registry
- Organization: Cantonal commercial registers
- URL: https://www.zefix.ch/ (links to cantonal registers)
- License: paid extracts
- Access: paid
- Freshness: real-time
- Record shape: certified extract (PDF) / register journal
- Primary keys: `uid`
- Join keys: `uid`, `chid`

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| officers[] | Personen | Officers + signing rights | array | person | planning-only; PII |
| capital | Kapital | Registered share capital | decimal | financial | planning-only; not statements |
| journal[] | Tagebuch | Register journal | array | filing | planning-only |
| purpose_full | Zweck | Full statutory purpose | string | metadata | planning-only |

## Interpretation Notes

- The authoritative route to **officers/signing rights**, **registered capital**,
  and the **full register journal** — **paid** per extract. Keep planning-only.
- Officers/signatories are personal data (FADP/GDPR). Much of the open subset
  (name, address, purpose, legal form, capital) is already in Zefix/SOGC.
