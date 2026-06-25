# Qatar Company Profile — Mapping

Qatar has a **dual-registry** structure plus a separate exchange. The onshore **MoCI
Commercial Register** (CR number) is authoritative for the bulk of companies but is
**lookup-only / auth-gated** (no open bulk/API). The **QFC Public Register** (QFC Number)
covers **financial-centre firms** and is **browser-public but ASP.NET postback-driven**.
The **Qatar Stock Exchange (QSE)** covers **listed** companies (browser-public, portlet
AJAX, no clean open API). The open-data portal carries statistics, not a register. No
registry per-company values were captured; listed identity uses public-knowledge QSE
symbols.

## Identifiers

- **CR number** — onshore MoCI Commercial Registration number; primary onshore id (gated).
- **QFC Number** — QFC firm id (financial centre); browser-public, postback-gated.
- **QSE symbol** (e.g. `QNBK`) / **ISIN** (`QA…`) — listed-entity key (QSE).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.cr_number | moci_commercial_registration | cr_number | yes | MoCI | onshore; auth-gated |
| registration.qfc_number | qfc_public_register | qfc_number | yes | QFC | financial centre; postback |
| legal_identity.legal_name | moci_commercial_registration | establishment_name | no | MoCI > QFC > QSE | QFC/QSE name as fallback |
| legal_identity.legal_form | moci_commercial_registration | legal_form | no | MoCI | W.L.L./Q.P.S.C./… |
| status.status_text | moci_commercial_registration | status | no | MoCI / QFC | QFCA licensed for QFC firms |
| status.date_of_registration | qfc_public_register | date_of_registration | no | QFC / MoCI | Gregorian |
| activity.activities | moci_commercial_registration | activities | no | MoCI | ISIC-based |
| activity.qse_sector | qse_listed | sector | no | QSE | listed only |
| registered_location.address | qfc_public_register | address | no | QFC / MoCI | redact individual addresses |
| capital.capital_amount | moci_commercial_registration | capital | no | MoCI | QAR; gated |
| officers[] | qfc_public_register / moci_commercial_registration | approved_individual_full_name / owners,manager | no | — | **PERSONAL DATA — REDACT** |
| listing.symbol | qse_listed | symbol | yes | QSE | ticker |
| listing.isin | qse_listed | isin | yes | QSE | `QA…` |
| listing.sector | qse_listed | sector | no | QSE | |
| financial_statements[] | qse_listed | financial_statements | no | QSE | QAR; listed only |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Onshore identity / registration / capital / activities**: **MoCI CR** is authoritative
  (auth-gated). **Financial-centre firms**: **QFC** (postback). The two registries use
  **distinct identifiers** (CR number vs QFC Number); a firm is in one or the other.
- **Legal name**: prefer MoCI; fall back to the QFC firm name or the QSE listed name.
- **Listing + financials**: **QSE** only, for the listed subset; join to MoCI/QFC by
  company name (no shared numeric key across registries and the exchange).
- **Currency** QAR. **Language** Arabic primary / English secondary. **Dates** Gregorian.

## Missing / restricted

- No open bulk/API anywhere: MoCI is **lookup-only/auth-gated**, QFC is **postback-gated**,
  QSE is **AJAX** with no identified data endpoint. `data.gov.qa` is statistics only.
- **Owners / partners / managers** (MoCI) and **approved individuals** (QFC) are personal
  data under **Law No. 13 of 2016** — redact.
- Private-company financials are **not public**; only QSE-listed financials are.
