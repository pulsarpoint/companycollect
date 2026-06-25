# Qatar Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| QFC Public Register | qfc_public_register | QFCA | browser-public (postback) | HTML | useful_secondary_source | QFC-licensed firms + approved individuals |
| MoCI Commercial Registration | moci_commercial_registration | MoCI | lookup / auth-gated | HTML | blocked_by_authentication | Onshore companies registry (CR number) |
| Qatar Stock Exchange (listed) | qse_listed | QSE | browser-public (AJAX) | HTML | useful_secondary_source | Listed companies + financials/disclosures |
| Qatar Open Data Portal | data_gov_qa | PSA | public API | JSON/CSV | not_company_data | Statistics only — no company register |

## Notes

- **No open bulk file or API** for Qatari company data was found.
- **QFC Public Register** is the most structured open source but covers **only the financial
  centre**; it is ASP.NET postback-driven (grid empty on plain GET) — no clean GET/bulk/API.
- **MoCI** is the authoritative **onshore** registry (CR number) but is not openly
  downloadable; verification is a per-CR lookup, often Arabic, commonly behind the national
  portal. Field model from public knowledge only.
- **QSE** covers listed companies (browser-public Liferay portal; portlet AJAX, no clean
  open JSON API). Identifiers: ticker symbol + ISIN (QA…).
- **data.gov.qa** runs an Opendatasoft v2.1 API (1,405 datasets) but is **statistical**;
  no company/legal-entity register dataset.
- Personal data (approved individuals, owners, managers) must be redacted.
