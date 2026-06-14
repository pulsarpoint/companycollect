# CORE — Companies Online Registration Environment (company search) Field Catalog

## Source Summary

- Country: Ireland
- Source type: official_registry
- Organization: Companies Registration Office (CRO)
- URL: https://core.cro.ie/
- License: public (free search)
- Access: public
- Freshness: real-time
- Record shape: company search page (free name/number lookup; document purchase)
- Primary keys: `company_num`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_num | CRO number | Company id | string | identifier | — | real-time lookup |
| documents_list | company documents | Filed documents (purchase) | array | document | — | paid |

## Interpretation Notes

- **Real-time companion to the open data.** CORE is the CRO's online portal for **free company name/number
  search** and for **filing** submissions; filed documents can be **purchased**. For bulk/API needs use the Open
  Data Portal (Company Records + Financial-Statements index); CORE is for manual lookups and as an alternative
  document-purchase route. Field semantics mirror the open data.
