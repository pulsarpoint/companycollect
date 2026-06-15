# EDR — Legal Entities (UO) Field Catalog

## Source Summary

- Country: Ukraine
- Source type: official_registry
- Organization: Ministry of Justice of Ukraine via data.gov.ua
- URL: https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7 (resource UO.zip)
- License: CC-BY 4.0
- Access: public
- Freshness: weekly
- Record shape: **windows-1251** XML, `<SUBJECT>` records (UO.zip → UO.xml, 3.1 GB)
- Primary keys: `EDRPOU`
- Join keys: `EDRPOU`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| EDRPOU | EDRPOU | 8-digit code | string | identifier | 26535980 | join key |
| NAME / SHORT_NAME | NAME/SHORT_NAME | Full/short name | string | legal_name | ТОВ "…" | Ukrainian |
| OPF | OPF | Legal form | string | legal_form | ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ | ТОВ=LLC |
| STAN | STAN | Status | string | status | зареєстровано | registered/in-termination/terminated |
| FOUNDERS/FOUNDER | FOUNDER | Founders + share | array | ownership | (redacted) | PII |
| BENEFICIARIES/BENEFICIARY | BENEFICIARY | Beneficial owners | array | ownership | (redacted) | PII; open UBO |
| SUPERIOR_MANAGEMENT | SUPERIOR_MANAGEMENT | Governing body | string | metadata | ЗАГАЛЬНІ ЗБОРИ | |
| SIGNERS/SIGNER | SIGNER | Officers | array | person | (redacted) - керівник | PII |
| AUTHORIZED_CAPITAL | AUTHORIZED_CAPITAL | Share capital | decimal | financial | 1000,00 | UAH; comma |
| REGISTRATION | REGISTRATION | Reg. dates + no. | string | date | 29.04.2004; …; <no> | split on ';' |
| BRANCHES/BRANCH | BRANCH | Branches | array | relationship | | NAME+CODE |
| TERMINATED_INFO | TERMINATED_INFO | Termination | string | date | 05.12.2006; …; <reason> | dissolution |
| PREDECESSORS/ASSIGNEES | PREDECESSOR/ASSIGNEE | Related entities | array | relationship | | reorg links |
| EXCHANGE_DATA…TAX_PAYER_TYPE | TAX_PAYER_TYPE | Tax registrations | array | status | Реєстр платників податків | |

## Interpretation Notes

- **One of the world's most open registers**: **2,008,750** legal entities, free,
  CC-BY 4.0, weekly — including **beneficial owners**, **founders**, **officers**,
  and **authorized capital** openly.
- **Encoding windows-1251**; `&quot;`/`&apos;` entities; capital uses decimal comma.
  `UO.xml` is 3.1 GB — **stream** `<SUBJECT>` records.
- **Wartime reduction**: the current open export has **no registered address** and
  **no KVED activity code** — both removed for security since 2022. The full
  register (usr.minjust.gov.ua) is access-restricted.
- **PERSONAL DATA**: FOUNDERS / BENEFICIARIES / SIGNERS carry person names —
  redact in published outputs (the committed `sample_record.json` redacts them).
- Composite string fields (`REGISTRATION`, `TERMINATED_INFO`) need splitting on
  `;`. `TAX_PAYER_TYPE` signals tax/social-contribution registration (Ukraine has
  no separate VAT number).
- `sample_record.json` is a real record (EDRPOU 26535980), PII redacted.
