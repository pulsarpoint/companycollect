# Australia — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `ABN Bulk Extract data.gov.au Australian Business Register download XML ABN ACN entity name CC-BY`
- Result: ABR/ABN Lookup Bulk Extract on data.gov.au — free, **CC-BY 3.0 AU**, weekly XML in 2 zips; fields ABN, status/date, entity type, legal name, business/trading names, state/postcode, ACN/ARBN, GST, DGR. Free SOAP web services too.
- Decision: get the CKAN resource URLs and download.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `ASIC company register financial reports Australia access cost ASX listed company financial statements open data`
- Result: company financial reports lodged with ASIC, **bought per document** (ASIC Connect, paid). Only certain companies must lodge (public, large proprietary, disclosing entities). Listed lodge via ASX.
- Decision: financials = paid (ASIC) / listed (ASX).

## Attempt 3
- Date/time: 2026-06-15
- Source: curl (data.gov.au CKAN package_show abn-bulk-extract)
- Result: resources — bulkextract.xsd, readme PDF, `public_split_1_10.zip`, `public_split_11_20.zip`, resource-list CSV. License cc-by 3.0 AU.
- Decision: download the XSD + Part 1.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl + python (zipfile)
- Query: download XSD + Part 1 (492 MB); extract a company record
- Result: XSD elements (ABN, ASICNumber, EntityType, MainEntity/NonIndividualName, BusinessAddress State/Postcode, GST, DGR, OtherEntity). Real record: **QBE INSURANCE (INTERNATIONAL) LTD**, ABN 11000000948, ACN 000000948, Australian Public Company, ACT, NSW 2000, GST active. Part 1 zip = 20 XML files (Public01..10).
- Decision: ABR Bulk Extract = recommended identity source; build normalized sample.
