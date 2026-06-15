# Ukraine — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| EDR legal entities (UO) | edr_uo | official_registry | public | CC-BY 4.0 | xml/zip | recommended |
| EDR entrepreneurs (FOP) | edr_fop | official_registry | public | CC-BY 4.0 | xml/zip | useful_secondary_source |
| IFRS financial statements (XBRL) | xbrl_frs | official_financial | public | open | xbrl | recommended |
| NSSMC / SMIDA issuer financials | nssmc_smida | official_financial | public | open | xml/xbrl | useful_secondary_source |
| EDR full register (address/KVED) | edr_full_restricted | official_registry | restricted (wartime) | restricted | — | blocked_by_authentication |

## Best combination

**EDR UO** (open register: identity, status, founders, **beneficial owners**,
officers, capital — CC-BY 4.0) + **XBRL FRS / NSSMC-SMIDA** (financials for
IFRS reporters / issuers), joined on **EDRPOU**.

## Downloaded (real)

- `raw/bulk/uo.zip` — 325 MB (3.1 GB XML), **2,008,750** legal entities + metadata/sha256
- `raw/bulk/uo_schema.zip` + `UO_schema.xsd` — XML schema
- `raw/samples/uo_record_sample.xml` — one real record (PII present, local only)
- `normalized/companies.sample.jsonl` — one real record (PII redacted)
