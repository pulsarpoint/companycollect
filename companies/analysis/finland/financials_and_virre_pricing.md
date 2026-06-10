# Finland — Financial Statements And Virre Pricing

Date checked: 2026-06-10

## Summary

The current `finland/prhytj` company source does not include financial statement
figures. PRH financial data should be treated as a separate Finland source, not as
part of the YTJ company-register source.

Use the free PRH digital financial statement API first. Use Virre only as a paid
fallback for statements that are not available through the digital API.

## Free PRH digital financial statement API

Source: PRH Open Data digital financial statement information API.

Access:

- Public, no authentication observed.
- License: CC-BY-4.0.
- Structured access for digital financial statements filed in IXBRL.
- Discovery endpoints return statement metadata; statement detail is retrieved as
  XML.

Relevant endpoints:

```text
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/financials?businessId=<business_id>
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/financial?businessId=<business_id>&financialDate=<YYYY-MM-DD>
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financials?financialDate=<YYYY-MM-DD>
GET https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financial_statements?registeredDateStart=<YYYY-MM-DD>&registeredDateEnd=<YYYY-MM-DD>
```

Known limitation:

- PRH says only about 5% of all financial statements are filed digitally in IXBRL.
  The rest are available through Virre as paid documents.

Recommended internal source key:

```text
country: finland
source: prhxbrl
registry key: finland/prhxbrl
```

Recommended ingestion shape:

- Store raw XML and source metadata keyed by `business_id + financial_date`.
- Normalize common facts into ClickHouse tables only after sampling enough
  statement variants.
- Keep all raw facts/provenance so missing or unmapped financial taxonomy fields
  can be reprocessed later.

## Virre paid access

Virre is PRH's information service for official register documents. It can be used
to buy financial statement documents that are not available through the free digital
financial statement API.

Financial statement documents:

- Financial statement search is free.
- Electronic financial statement for a normal Virre user: EUR 4.02 per document.
- Electronic financial statement for a contract client: EUR 2.01 per document.
- Documents are the statements filed with the Trade Register and may be incomplete.
- This should be treated as paid PDF/document fallback, not the primary structured
  ingestion path.

Contract client charges:

| Item | Price |
|---|---:|
| Start-up fee for contract client | EUR 125.50 |
| Annual fee per user name | EUR 27.61 |
| Invoicing charge | EUR 8.16 |
| Electronic financial statement, contract client | EUR 2.01 per document |

Normal electronic Virre document/search pricing:

| Item | Price |
|---|---:|
| Company search | Free |
| Financial statements search | Free |
| Electronic financial statement | EUR 4.02 per document |
| Electronic Trade Register extract | Free |

Selection pricing:

| Selection route | Basic fee | Per information unit |
|---|---:|---:|
| Portal selection run by registered/contract client | EUR 150.60 | EUR 0.15 |
| Ordered selection / non-portal route | EUR 301.20 | EUR 0.15 |

All prices above are the PRH selling prices shown on the Virre price list checked
on 2026-06-10. PRH lists VAT at 25.5% for chargeable electronic Virre products in
the price table.

Rough break-even:

- Normal user pays EUR 4.02 per statement.
- Contract client pays EUR 2.01 per statement plus fixed fees.
- First-year fixed contract costs for one user are EUR 153.11 before invoicing
  charges (`125.50 + 27.61`).
- Ignoring invoicing charges, the first-year break-even is about 77 statements:
  `153.11 / (4.02 - 2.01)`.
- Including one invoicing charge, break-even is about 81 statements:
  `(153.11 + 8.16) / 2.01`.

## Implementation decision

For Corpscout/company sources:

1. Add `finland/prhxbrl` as a separate free structured source.
2. Import PRH XBRL source metadata and raw statement XML into ClickHouse.
3. Normalize common metrics after sampling.
4. Add Virre as a separate paid/manual or paid-automation fallback only if we need
   coverage beyond the digital IXBRL subset.

Do not put Virre PDF purchasing into the primary open-data import path.

## Sources

- PRH Virre price list:
  `https://www.prh.fi/en/companiesandorganisations/tietopalvelut/virre/virre_-_price_list.html`
- PRH Virre overview:
  `https://www.prh.fi/en/companiesandorganisations/tietopalvelut/virre.html`
- PRH Virre registration / contract client info:
  `https://www.prh.fi/en/companiesandorganisations/tietopalvelut/virre/register_as_a_user_or_become_a_contract_client.html`
- PRH Open Data overview:
  `https://avoindata.prh.fi/en`
- PRH XBRL OpenAPI schema:
  `https://avoindata.prh.fi/opendata-xbrl-api/v3/schema?lang=en`
