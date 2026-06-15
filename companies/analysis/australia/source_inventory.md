# Australia — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| ABR / ABN Lookup Bulk Extract | abr_bulk_extract | official_registry | public | CC-BY 3.0 AU | xml/zip | recommended |
| ABN Lookup web services | abn_lookup_api | official_registry | public (free GUID) | CC-BY 3.0 AU | json/soap | useful_secondary_source |
| ASIC company register (ASIC Connect) | asic_company_register | official_registry | paid | paid extracts | html/pdf | blocked_by_payment |
| ASIC financial reports | asic_financial_reports | official_financial | paid | paid per document | pdf | blocked_by_payment |
| ASX listed-company financials | asx_listed | stock_exchange | public (per-issuer) | issuer disclosure | pdf | useful_secondary_source |

## Best combination

**ABR Bulk Extract** (free, CC-BY 3.0 AU, all ABN holders) for identity, keyed on
**ABN** (+ ACN for companies); enrich per-ABN via the free **ABN Lookup API**.
Company-register detail (registered office, incorporation date) and financials are
**paid** via ASIC; listed-company financials via **ASX**.

## Downloaded (real)

- `raw/bulk/public_split_1_10.zip` — 492 MB (20 XML files) + metadata/sha256
- `raw/bulk/bulkextract.xsd` — schema
- `raw/samples/abr_company_record.xml` — real company record (QBE Insurance (International) Ltd, ABN 11000000948)
- `normalized/companies.sample.jsonl` — one real normalized record
