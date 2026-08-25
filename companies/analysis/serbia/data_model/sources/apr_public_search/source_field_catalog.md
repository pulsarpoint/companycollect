# APR Public Company Search Field Catalog

> **SAMPLE-ONLY / MANUAL UI EVIDENCE.** One company was inspected manually on
> 2026-08-25 after the user completed the CAPTCHA. APR prohibits automated
> collection from this interface. No personal name or personal identifier was
> copied into the repository.

## Source Summary

- Source: APR Public Company Search
- URL: https://pretraga.apr.gov.rs/search
- Access: manual public search; no authentication; CAPTCHA protected
- Intended use here: validate field semantics before receiving the paid SP3/SP4 schema
- Primary/join key: `maticni_broj`

## Observed Representative Shape

| Normalized path | Visible APR label | Type | Modeling note |
|---|---|---|---|
| `legal_representatives[].party_kind` | Физичка лица законски заступници | string | derived from section; observed natural person |
| `legal_representatives[].name` | Име и презиме | string | personal data; inspected value redacted |
| `legal_representatives[].function_title` | Функција | string | observed value `Директор`; retain raw label and map to `director` |
| `legal_representatives[].personal_identifier` | ЈМБГ | string | reveal control present; value not revealed; never store raw |
| `legal_representatives[].represents_independently` | Самостално заступа | boolean | observed `Да` → `true` |
| `representative_sections[].relationship_kind` | section heading | string | sections include other representatives, directors, boards, procurists and group procura |
| `members[]` | Чланови | array | company members are not automatically beneficial owners |

## Interpretation

The public UI supplies enough semantic evidence to design the representative
tables, but it is not an ingestion source. The exact SP3/SP4 payload field names,
identifiers, history semantics and change-feed envelope remain unknown until APR
provides the paid delivery/web-service specification.

JMBG must never be persisted raw. If a lawful identity-linking use case is
approved, transform it before loading with a secret-keyed HMAC-SHA256. A plain
hash is reversible by enumeration because the identifier space is small.
